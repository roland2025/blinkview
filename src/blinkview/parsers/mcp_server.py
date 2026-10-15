# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import re
import secrets
import threading

from blinkview.core.agent_api import AgentApiError, AgentApiServer
from blinkview.core.agent_log_read import current_sequence, list_modules, read_logs, summary
from blinkview.core.agent_telemetry import telemetry_latest, telemetry_series
from blinkview.core.config_schema import (
    CONFIG_ROOTS,
    WATCHES_ROOT,
    ConfigPatchError,
    changed_entries,
    check_change,
    check_gui_change,
    for_agent,
    schema_at,
)
from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.parser import BaseParser, ParserFactory
from blinkview.utils.generate_id import generate_id
from blinkview.utils.log_level import LogLevel

MAX_SEND_CHARS = 4096
# get_config's answer as JSON text. Above it the agent is told to narrow the path or pass 'depth'.
# Also the most a patch given to apply_config_patch may be.
MAX_CONFIG_CHARS = 50_000
# Changes whose undo token still works; older ones are forgotten.
MAX_UNDO_TOKENS = 50

_MISSING = object()

# Ends the answer to a change the pipeline does not allow: the agent can usually reach the
# profile's files too, and must not take that way round.
_NO_FILE_EDIT_HINT = (
    "Do not edit the profile's files directly instead: the user turned this off. "
    "Tell them what you wanted to change; they can allow it in this pipeline's settings."
)

# Key prefixes of the watch entries that need a key, as the watch window generates them.
_WATCH_KEY_PREFIXES = {"row": "row", "button": "btn", "group_start": "grp_start", "group_end": "grp_end"}

# `post`: the most `values` one call may log, and the levels it takes by name.
MAX_POST_VALUES = 50
_POST_LEVELS = {level.name_conf: level for level in LogLevel.LIST_CONF}
_POST_LEVELS["WARN"] = LogLevel.WARN
# A module path as DeviceIdentity.get_module takes it, without empty segments.
_MODULE_PATH = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")
# Where the agent's sends and commands are logged on this pipeline's device: tx.<source name>.
TX_MODULE = "tx"


@ParserFactory.register("mcp_server")
@override_property("sources_", hidden=True, required=False)
@override_property("delay", hidden=True)
@configuration_property(
    "send_to_any",
    title="Agent may send to any source",
    type="boolean",
    default=True,
    required=True,
    ui_order=2,
    description="Every source accepts text and commands from the agent. Turn off to limit it to the list below.",
)
@configuration_property(
    "send_to",
    title="Only these sources",
    type="array",
    required=True,
    items={"type": "string", "_reference": "/sources"},
    default=[],
    ui_order=3,
    description="Used when 'Agent may send to any source' is off: only these accept text and commands. "
    "Empty: the agent can send nothing.",
)
@configuration_property(
    "allow_rotate",
    title="Agent may start a new session",
    type="boolean",
    default=True,
    required=True,
    ui_order=4,
    description="Lets the agent end the current session and start a new one (same as the Rotate button).",
)
@configuration_property(
    "allow_read",
    title="Agent may read logs, telemetry and config",
    type="boolean",
    default=True,
    required=True,
    ui_order=5,
    description="Lets the agent read this session's log lines, module list and telemetry values, "
    "and this profile's configuration.",
)
@configuration_property(
    "allow_config",
    title="Agent may change the configuration",
    type="boolean",
    default=False,
    required=True,
    ui_order=6,
    description="Lets the agent change this profile's sources, pipelines and storage settings; the change is "
    "saved to the profile like an edit made here. Each one is checked first and shown with an Undo button. "
    "It cannot change this pipeline's own permissions.",
)
@configuration_property(
    "allow_watches",
    title="Agent may create and change watch lists",
    type="boolean",
    default=True,
    required=True,
    ui_order=7,
    description="Lets the agent create, change and open watch lists (Watches menu). Each change is shown "
    "with an Undo button.",
)
@configuration_property(
    "allow_post",
    title="Agent may log its progress",
    type="boolean",
    default=True,
    required=True,
    ui_order=8,
    description="Lets the agent log lines and values of its own under this pipeline's name, on modules it "
    "names - to follow what it is doing in the log, telemetry and watch windows. Its sends and commands "
    "are logged there either way, under 'tx'.",
)
@configuration_property(
    "port",
    type="integer",
    default=0,
    ui_order=9,
    description="Local TCP port to listen on (127.0.0.1 only). 0 picks a free one; `blink mcp` finds it either way.",
)
class McpServer(BaseParser):
    __doc__ = """Lets an AI agent (Claude, through `blink mcp`) act on this running BlinkView.

* Sends text and source commands (reset, halt, ...) to any source, or only to those listed in `send_to`.
* Starts a new session.
* Reads log lines, through the log viewer's own filters and formatter, and counts them per module and level.
* Reads telemetry: the latest value per module, and a module's numeric history.
* Reads this profile's configuration (sources, pipelines, ...) as it is in effect now, and its schema.
* Changes the configuration, when `allow_config` is on: checked against the schema, with Undo.
* Creates, changes and opens watch lists.
* Logs the agent's progress (`post`) and its sends and commands (under `tx`) as lines of this pipeline's
  device, for the log, telemetry and watch windows, and the session's recording.
* Listens on 127.0.0.1 only and requires a per-run token; everything it does is logged.

It is not a parser: no data flows through it; its only lines are the agent's own.
Connect an agent with `claude mcp add blinkview -- blink mcp`."""

    send_to_any: bool
    send_to: list
    allow_rotate: bool
    allow_read: bool
    allow_config: bool
    allow_watches: bool
    allow_post: bool
    port: int

    def __init__(self):
        super().__init__()
        # token -> (ConfigManager, target, undo patch), oldest first; see _remember_undo().
        self._undo_patches = {}
        self._undo_lock = threading.Lock()

    def run(self):
        logger = self.logger
        fm = self.shared.registry.file_manager
        project_dir = getattr(fm, "_project_dir", None)
        try:
            server = AgentApiServer(
                {
                    "info": self._tool_info,
                    "send": self._tool_send,
                    "command": self._tool_command,
                    "rotate_session": self._tool_rotate_session,
                    "read_logs": self._tool_read_logs,
                    "summary": self._tool_summary,
                    "list_modules": self._tool_list_modules,
                    "telemetry_latest": self._tool_telemetry_latest,
                    "telemetry_series": self._tool_telemetry_series,
                    "get_config": self._tool_get_config,
                    "get_schema": self._tool_get_schema,
                    "list_types": self._tool_list_types,
                    "apply_config_patch": self._tool_apply_config_patch,
                    "create_watch": self._tool_create_watch,
                    "open_view": self._tool_open_view,
                    "post": self._tool_post,
                },
                port=self.port,
                run_info={
                    "project": str(project_dir) if project_dir else None,
                    "profile": fm.profile_name,
                    "name": self.name,
                },
            )
        except OSError as e:
            logger.error("Could not start on port %s: %s", self.port, e)
            return

        logger.info("Listening on 127.0.0.1:%s", server.port)
        server.serve_until(self._stop_event.is_set)

    # --- Tools (each runs on a request thread) ---

    def _allowed_sources(self) -> dict:
        """{source_id: source} the agent may send to: every source, or the `send_to` ones."""
        sources = self.shared.registry.sources
        if self.send_to_any:
            return dict(sources.sources) if sources is not None else {}
        allowed = {}
        for source_id in self.send_to or []:
            source = sources.get(source_id) if sources is not None else None
            if source is not None:
                allowed[source_id] = source
        return allowed

    def _resolve_source(self, args: dict):
        allowed = self._allowed_sources()
        if not allowed:
            raise AgentApiError(f"no source to send to (see 'send_to_any' / 'send_to' on the '{self.name}' pipeline)")

        wanted = args.get("source")
        if wanted is None:
            if len(allowed) == 1:
                return next(iter(allowed.items()))
            raise AgentApiError(f"'source' is required, one of: {self._source_names(allowed)}")

        if wanted in allowed:
            return wanted, allowed[wanted]
        by_name = [(sid, src) for sid, src in allowed.items() if getattr(src, "name", None) == wanted]
        if len(by_name) == 1:
            return by_name[0]
        if by_name:
            raise AgentApiError(f"several sources are named '{wanted}', use the id: {[sid for sid, _ in by_name]}")
        raise AgentApiError(f"unknown or not allowed source '{wanted}', allowed: {self._source_names(allowed)}")

    @staticmethod
    def _source_names(allowed: dict) -> list:
        return [getattr(src, "name", sid) for sid, src in allowed.items()]

    def _tool_info(self, args: dict) -> dict:
        registry = self.shared.registry
        fm = registry.file_manager
        sources = []
        for source_id, source in self._allowed_sources().items():
            source_config = registry.config.get_by_path(f"/sources/{source_id}", {})
            sources.append(
                {
                    "id": source_id,
                    "name": getattr(source, "name", source_id),
                    "type": source_config.get("type") or type(source).__name__,
                    "enabled": bool(getattr(source, "enabled", False)),
                    "can_send": hasattr(source, "send_data"),
                    "commands": {token: label for token, label in source.get_commands()},
                }
            )
        return {
            "project_name": fm.project_name,
            "profile": fm.profile_name,
            "session_id": fm.session_dir.name,
            "session_name": fm.session_display_name,
            "session_dir": str(fm.session_dir),
            "can_rotate": bool(self.allow_rotate) and not registry.replay_mode,
            "can_read": bool(self.allow_read),
            "can_configure": bool(self.allow_config),
            "can_edit_watches": bool(self.allow_watches) and registry.gui_config is not None,
            "can_post": bool(self.allow_post),
            "post_device": self.local.device_id.name,
            "seq": current_sequence(registry),
            "sources": sources,
        }

    def _tool_send(self, args: dict) -> dict:
        source_id, source = self._resolve_source(args)
        text = args.get("text")
        if not isinstance(text, str) or not text:
            raise AgentApiError("'text' must be a non-empty string")
        if len(text) > MAX_SEND_CHARS:
            raise AgentApiError(f"'text' is longer than {MAX_SEND_CHARS} characters")
        if not hasattr(source, "send_data"):
            raise AgentApiError(f"source '{source_id}' cannot send data")

        payload = text + "\n" if args.get("newline", True) else text
        # Taken before writing: reading from here on returns the device's reply and nothing older.
        seq = current_sequence(self.shared.registry)
        self.logger.info("send to '%s': %r", source_id, payload)
        self._log_tx(source_id, source, "send: " + text.replace("\r", "\\r").replace("\n", "\\n"))
        written = source.send_data(payload)

        # Readers that know report bytes written (RTT); 0 then means it never left this PC.
        if written == 0:
            raise AgentApiError(f"nothing was written to '{source_id}': not connected, or its buffer is full")
        result = {"source": source_id, "bytes": len(payload.encode()), "written": written, "seq": seq}

        if args.get("wait_s") and self.allow_read:
            # 'text' is what was sent, not a log filter; everything else is read_logs' own.
            read_args = {
                k: v
                for k, v in args.items()
                if k not in ("text", "source", "newline", "last_n", "around", "before", "after")
            }
            read_args["after_seq"] = seq
            try:
                result["reply"] = read_logs(self.shared.registry, read_args, self._stop_event.is_set)
            except AgentApiError as e:
                result["reply_error"] = str(e)  # the text was sent all the same
        return result

    def _require_read(self):
        if not self.allow_read:
            raise AgentApiError(f"reading logs is turned off on the '{self.name}' pipeline")

    def _tool_list_modules(self, args: dict) -> dict:
        self._require_read()
        return list_modules(self.shared.registry, args)

    def _tool_telemetry_latest(self, args: dict) -> dict:
        self._require_read()
        return telemetry_latest(self.shared.registry, args)

    def _tool_telemetry_series(self, args: dict) -> dict:
        self._require_read()
        return telemetry_series(self.shared.registry, args)

    def _tool_read_logs(self, args: dict) -> dict:
        if not self.allow_read:
            raise AgentApiError(f"reading logs is turned off on the '{self.name}' pipeline")
        return read_logs(self.shared.registry, args, self._stop_event.is_set)

    def _tool_summary(self, args: dict) -> dict:
        self._require_read()
        return summary(self.shared.registry, args)

    def _tool_get_config(self, args: dict) -> dict:
        self._require_read()
        path = args.get("path", "/")
        if not isinstance(path, str) or not path.startswith("/"):
            raise AgentApiError("'path' must start with '/', e.g. '/pipelines'")
        depth = args.get("depth")
        if depth is not None and (not isinstance(depth, int) or isinstance(depth, bool) or depth < 1):
            raise AgentApiError("'depth' must be an integer of 1 or more")

        # A copy: the answer is serialized after the config's lock is released.
        manager, target = self._config_target(args, write=False)
        config = manager.get_by_path(path, _MISSING, make_deep_copy=True, depth=depth)
        if config is _MISSING:
            raise AgentApiError(f"nothing at '{path}' in the {target} configuration")

        size = len(json.dumps(config))
        if size > MAX_CONFIG_CHARS:
            raise AgentApiError(
                f"the configuration at '{path}' is {size} characters (max {MAX_CONFIG_CHARS}): "
                "pass 'depth' for an outline, or a path further down"
            )
        return {"path": path, "config": config}

    def _tool_list_types(self, args: dict) -> dict:
        self._require_read()
        factories = self.shared.factories
        category = args.get("category")
        if category is None:
            return {
                "config_paths": {
                    f"/{root}/<id>" if by_id else f"/{root}": name for root, (name, by_id) in CONFIG_ROOTS.items()
                },
                "categories": {name: self._type_names(name) for name in factories.get_categories()},
            }
        if not isinstance(category, str) or factories.get_factory(category) is None:
            raise AgentApiError(f"unknown category '{category}', one of: {factories.get_categories()}")
        # The first paragraph of each type's description (it may wrap); get_schema has the whole of it.
        types = {
            name: " ".join(doc.strip().split("\n\n", 1)[0].split())
            for name, doc in factories.get_category_types(category)
        }
        return {"category": category.lower(), "types": types}

    def _type_names(self, category: str) -> list:
        return [name for name, _doc in self.shared.factories.get_category_types(category)]

    def _tool_get_schema(self, args: dict) -> dict:
        self._require_read()
        factories = self.shared.factories
        path, category, type_name = args.get("path"), args.get("category"), args.get("type")
        if path is not None:
            if category is not None or type_name is not None:
                raise AgentApiError("pass either 'path' or 'category' and 'type', not both")
            if not isinstance(path, str) or not path.startswith("/"):
                raise AgentApiError("'path' must start with '/', e.g. '/pipelines/<id>'")
            try:
                schema = for_agent(schema_at(factories, self.shared.registry.config.get_full_config(), path))
            except ConfigPatchError as e:
                raise AgentApiError(str(e)) from None
            result = {"path": path, "schema": schema}
        else:
            if not isinstance(category, str) or factories.get_factory(category) is None:
                raise AgentApiError(
                    f"pass 'path', or a 'category' (one of: {factories.get_categories()}) and a 'type' of it"
                )
            if not isinstance(type_name, str) or type_name.lower() not in self._type_names(category):
                raise AgentApiError(f"unknown type '{type_name}' in '{category}', one of: {self._type_names(category)}")
            schema = for_agent(factories.get_schema(category, type_name))
            result = {"category": category.lower(), "type": type_name.lower(), "schema": schema}

        size = len(json.dumps(schema))
        if size > MAX_CONFIG_CHARS:
            raise AgentApiError(
                f"the schema is {size} characters (max {MAX_CONFIG_CHARS}): ask for a path further down"
            )
        return result

    def _config_target(self, args: dict, write: bool):
        """(ConfigManager, name) of a tool's `target`: the profile, or the GUI's own config
        (the watch lists). With `write`, refused unless the pipeline allows changing it."""
        target = args.get("target", "profile")
        registry = self.shared.registry
        if target == "profile":
            if write and not self.allow_config:
                raise AgentApiError(
                    f"changing the configuration is turned off on the '{self.name}' pipeline. {_NO_FILE_EDIT_HINT}"
                )
            return registry.config, target
        if target == "gui":
            if write and not self.allow_watches:
                raise AgentApiError(
                    f"changing watch lists is turned off on the '{self.name}' pipeline. {_NO_FILE_EDIT_HINT}"
                )
            if registry.gui_config is None:
                raise AgentApiError("this BlinkView runs without a window: it has no watch lists")
            return registry.gui_config, target
        raise AgentApiError("'target' must be 'profile' or 'gui'")

    def _known_modules(self) -> set:
        """'device.module' of every module seen so far - a lookup that registers nothing."""
        return {module.name_with_device() for module in self.shared.id_registry.get_all_modules()}

    def _remember_undo(self, manager, target: str, undo: list) -> str:
        """A token for `undo`, the patch reverting a change: what the agent gets instead of the
        patch itself, which for a changed list is the whole list twice."""
        token = secrets.token_hex(4)
        with self._undo_lock:
            self._undo_patches[token] = (manager, target, undo)
            while len(self._undo_patches) > MAX_UNDO_TOKENS:
                del self._undo_patches[next(iter(self._undo_patches))]
        return token

    def _undo_by_token(self, token, dry_run: bool) -> dict:
        with self._undo_lock:
            remembered = self._undo_patches.get(token) if isinstance(token, str) else None
        if remembered is None:
            raise AgentApiError(
                f"no change with undo token {token!r}: it was undone already, or is older than the last "
                f"{MAX_UNDO_TOKENS} changes of this run"
            )
        manager, target, undo = remembered
        self._config_target({"target": target}, write=True)
        try:
            # The undo patch tests what it replaces, so it is refused once that was edited again.
            result = self._apply_patch(manager, target, "/", undo, dry_run)
        except AgentApiError as e:
            raise AgentApiError(f"could not undo: {e}") from None
        if result["applied"]:
            with self._undo_lock:
                self._undo_patches.pop(token, None)
        return result

    def _tool_apply_config_patch(self, args: dict) -> dict:
        dry_run = args.get("dry_run", False)
        if not isinstance(dry_run, bool):
            raise AgentApiError("'dry_run' must be true or false")
        if args.get("undo") is not None:
            if args.get("patch") is not None:
                raise AgentApiError("pass either 'patch' or 'undo', not both")
            return self._undo_by_token(args["undo"], dry_run)

        manager, target = self._config_target(args, write=True)
        path, patch = args.get("path", "/"), args.get("patch")
        try:
            size = len(json.dumps(patch))
        except (TypeError, ValueError):
            raise AgentApiError("'patch' must be a list of JSON Patch operations") from None
        if size > MAX_CONFIG_CHARS:
            raise AgentApiError(f"'patch' is {size} characters (max {MAX_CONFIG_CHARS}): apply it in parts")
        return self._apply_patch(manager, target, path, patch, dry_run)

    def _apply_patch(self, manager, target: str, path: str, patch: list, dry_run: bool = False) -> dict:
        registry = self.shared.registry
        changed, warnings = [], []

        def check(before: dict, after: dict):
            if target == "gui":
                warnings.extend(check_gui_change(before, after, self._known_modules()))
            else:
                warnings.extend(check_change(self.shared.factories, before, after))
            changed.extend(changed_entries(before, after))

        # Taken before the change, like send's: what the change does to the log is read from here on.
        seq = current_sequence(registry)
        try:
            undo = manager.apply_patch_checked(path, patch, check=check, dry_run=dry_run)
        except ConfigPatchError as e:
            raise AgentApiError(str(e)) from None

        result = {"applied": bool(undo) and not dry_run, "changed": changed, "warnings": warnings, "seq": seq}
        if not undo:
            result["note"] = "the patch changes nothing"
        elif not dry_run:
            result["undo"] = self._remember_undo(manager, target, undo)
            self.logger.info(
                "%s config changed: %s, patch at '%s': %s", target, ", ".join(changed), path, json.dumps(patch)
            )
            registry.notify_user(
                f"Agent changed {', '.join(changed)}",
                "Undo",
                # Clicked on the UI thread; applying a config may restart a source or pipeline.
                lambda: self.shared.tasks.run_task(self._undo_config_change, manager, undo, changed),
            )
        return result

    def _undo_config_change(self, manager, undo: list, changed: list):
        """The Undo button of a change's notice. The undo patch tests what it is about to
        replace or remove, so it is refused as a whole once the same place was edited again."""
        registry = self.shared.registry
        try:
            manager.apply_patch_checked("/", undo)
        except ConfigPatchError as e:
            self.logger.warning("could not undo the change to %s: %s", ", ".join(changed), e)
            registry.notify_user(f"Could not undo the change to {', '.join(changed)}: it was edited since")
            return
        self.logger.info("config change undone: %s", ", ".join(changed))
        registry.notify_user(f"Undone: {', '.join(changed)}")

    def _tool_create_watch(self, args: dict) -> dict:
        manager, target = self._config_target({"target": "gui"}, write=True)
        name, entries, open_it = args.get("name"), args.get("entries", []), args.get("open", True)
        if not isinstance(name, str) or not name.strip():
            raise AgentApiError("'name' must be a non-empty string")
        if not isinstance(entries, list):
            raise AgentApiError("'entries' must be a list")
        if not isinstance(open_it, bool):
            raise AgentApiError("'open' must be true or false")

        # Keys the agent left out are generated, like the watch window does for a new entry.
        used = {entry.get("key") for entry in entries if isinstance(entry, dict)}
        filled = []
        for entry in entries:
            if isinstance(entry, dict):
                entry = {"type": "row", **entry}
                prefix = _WATCH_KEY_PREFIXES.get(entry["type"]) if isinstance(entry["type"], str) else None
                if prefix and "key" not in entry:
                    entry["key"] = generate_id(prefix, list(used))
                    used.add(entry["key"])
            filled.append(entry)

        watches = manager.get_by_path(f"/{WATCHES_ROOT}")
        watch_id = generate_id("watch", list(watches or {}))
        watch = {"id": watch_id, "name": name.strip(), "entries": filled}
        if isinstance(watches, dict):
            patch = [{"op": "add", "path": f"/{WATCHES_ROOT}/{watch_id}", "value": watch}]
        else:
            patch = [{"op": "add", "path": f"/{WATCHES_ROOT}", "value": {watch_id: watch}}]

        result = self._apply_patch(manager, target, "/", patch)
        result["id"] = watch_id
        result["opened"] = open_it and self.shared.registry.open_view("watch", watch_id)
        return result

    def _tool_open_view(self, args: dict) -> dict:
        manager, _target = self._config_target({"target": "gui"}, write=False)
        kind, view_id = args.get("kind", "watch"), args.get("id")
        if kind != "watch":
            raise AgentApiError("'kind' must be 'watch'")
        watches = manager.get_by_path(f"/{WATCHES_ROOT}") or {}
        if view_id not in watches:
            raise AgentApiError(f"no watch '{view_id}', have: {sorted(watches)}")
        return {"kind": kind, "id": view_id, "opened": self.shared.registry.open_view(kind, view_id)}

    def _tool_command(self, args: dict) -> dict:
        source_id, source = self._resolve_source(args)
        command = args.get("command")
        commands = {token: label for token, label in source.get_commands()}
        if command not in commands:
            raise AgentApiError(f"unknown command '{command}' for '{source_id}', available: {sorted(commands)}")

        # Taken before the command, like send's: a reset's boot log is read from here on.
        seq = current_sequence(self.shared.registry)
        self.logger.info("command to '%s': %s", source_id, command)
        self._log_tx(source_id, source, f"command: {command}")
        source.send_command(command)
        return {"source": source_id, "command": command, "seq": seq}

    def _tool_rotate_session(self, args: dict) -> dict:
        if not self.allow_rotate:
            raise AgentApiError(f"starting a new session is turned off on the '{self.name}' pipeline")
        name = args.get("name")
        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise AgentApiError("'name' must be a non-empty string when given")

        self.logger.info("rotate session (name=%r)", name)
        try:
            old_dir, new_dir = self.shared.registry.request_session_rotation(name.strip() if name else None)
        except (RuntimeError, TimeoutError) as e:
            raise AgentApiError(str(e)) from None
        return {"old_session_id": old_dir.name, "session_id": new_dir.name, "session_dir": str(new_dir)}

    # --- The agent's own lines ---

    def _tool_post(self, args: dict) -> dict:
        if not self.allow_post:
            raise AgentApiError(f"logging the agent's progress is turned off on the '{self.name}' pipeline")

        module = args.get("module")
        module = module.strip().lower() if isinstance(module, str) else None
        if not module or not _MODULE_PATH.match(module):
            raise AgentApiError("'module' must be a dotted path of letters, digits and '_', e.g. 'dut2.flash'")

        level_name = args.get("level", "INFO")
        level = _POST_LEVELS.get(level_name.upper()) if isinstance(level_name, str) else None
        if level is None:
            raise AgentApiError(f"'level' must be one of: {sorted(_POST_LEVELS)}")

        rows = []
        if args.get("text") is not None:
            rows.append((module, self._one_line(args["text"], "'text'")))

        values = args.get("values", {})
        if not isinstance(values, dict):
            raise AgentApiError("'values' must be an object of key: number or string")
        if len(values) > MAX_POST_VALUES:
            raise AgentApiError(f"'values' has {len(values)} entries (max {MAX_POST_VALUES}): post them in parts")
        for key, value in values.items():
            path = f"{module}.{key.strip().lower()}"
            if not _MODULE_PATH.match(path):
                raise AgentApiError(f"value key {key!r} must be letters, digits and '_' (dots for submodules)")
            if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                raise AgentApiError(f"value {key!r} must be a number or a string")
            rows.append((path, self._one_line(value, repr(key)) if isinstance(value, str) else format(value, ".10g")))

        if not rows:
            raise AgentApiError("pass 'text', 'values' or both")

        seq = current_sequence(self.shared.registry)
        self._log_rows(rows, level)
        device = self.local.device_id.name
        return {"device": device, "rows": [f"{device}.{path}" for path, _text in rows], "seq": seq}

    @staticmethod
    def _one_line(text, what: str) -> str:
        if not isinstance(text, str):
            raise AgentApiError(f"{what} must be a string")
        if len(text) > MAX_SEND_CHARS:
            raise AgentApiError(f"{what} is longer than {MAX_SEND_CHARS} characters")
        if "\n" in text or "\r" in text:
            raise AgentApiError(f"{what} must be one line: post each line on its own")
        return text

    def _log_tx(self, source_id: str, source, text: str):
        """The agent's send or command as a line at tx.<source name>, logged before it is carried
        out so that it sorts before the device's reply."""
        name = re.sub(r"[^a-z0-9_]", "_", str(getattr(source, "name", None) or source_id).lower())
        try:
            self._log_rows([(f"{TX_MODULE}.{name}", text)], LogLevel.INFO)
        except Exception as e:  # the send itself must not fail over its log line
            self.logger.warning("could not log the agent's line for '%s': %s", source_id, e)

    def _log_rows(self, rows: list, level):
        """[(module path, message)] as lines of this pipeline's device, all at one timestamp.
        Runs on a request thread: the batch is this call's own."""
        device = self.local.device_id
        encoded = [(device.get_module(path).id, text.encode("utf-8")) for path, text in rows]
        batch = self.shared.array_pool.create(
            PooledLogBatch,
            len(encoded),
            max(sum(len(msg) for _module_id, msg in encoded), 1),
            has_levels=True,
            has_modules=True,
            has_devices=True,
        )
        now_ns = self.shared.time_ns()
        with batch:
            for module_id, msg in encoded:
                batch.insert(now_ns, now_ns, msg, level=level.value, module=module_id, device=device.id)
            self.distribute(batch)
