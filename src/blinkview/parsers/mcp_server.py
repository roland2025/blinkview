# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json

from blinkview.core.agent_api import AgentApiError, AgentApiServer
from blinkview.core.agent_log_read import current_sequence, list_modules, read_logs, summary
from blinkview.core.agent_telemetry import telemetry_latest, telemetry_series
from blinkview.core.config_schema import (
    CONFIG_ROOTS,
    ConfigPatchError,
    changed_entries,
    check_change,
    for_agent,
    schema_at,
)
from blinkview.core.configurable import configuration_property, override_property
from blinkview.parsers.parser import BaseParser, ParserFactory

MAX_SEND_CHARS = 4096
# get_config's answer as JSON text. Above it the agent is told to narrow the path or pass 'depth'.
# Also the most a patch given to apply_config_patch may be.
MAX_CONFIG_CHARS = 50_000

_MISSING = object()


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
    "port",
    type="integer",
    default=0,
    ui_order=7,
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
* Listens on 127.0.0.1 only and requires a per-run token; everything it does is logged.

It is not a parser: no data flows through it. Connect an agent with `claude mcp add blinkview -- blink mcp`."""

    send_to_any: bool
    send_to: list
    allow_rotate: bool
    allow_read: bool
    allow_config: bool
    port: int

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
        config = self.shared.registry.config.get_by_path(path, _MISSING, make_deep_copy=True, depth=depth)
        if config is _MISSING:
            raise AgentApiError(f"nothing at '{path}' in the profile's configuration")

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
        # The first line of each type's description; get_schema has the whole of it.
        types = {name: doc.strip().split("\n", 1)[0] for name, doc in factories.get_category_types(category)}
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

    def _tool_apply_config_patch(self, args: dict) -> dict:
        if not self.allow_config:
            raise AgentApiError(f"changing the configuration is turned off on the '{self.name}' pipeline")
        path, patch, dry_run = args.get("path", "/"), args.get("patch"), args.get("dry_run", False)
        if not isinstance(dry_run, bool):
            raise AgentApiError("'dry_run' must be true or false")
        try:
            size = len(json.dumps(patch))
        except (TypeError, ValueError):
            raise AgentApiError("'patch' must be a list of JSON Patch operations") from None
        if size > MAX_CONFIG_CHARS:
            raise AgentApiError(f"'patch' is {size} characters (max {MAX_CONFIG_CHARS}): apply it in parts")

        registry = self.shared.registry
        changed, warnings = [], []

        def check(before: dict, after: dict):
            warnings.extend(check_change(self.shared.factories, before, after))
            changed.extend(changed_entries(before, after))

        # Taken before the change, like send's: what the change does to the log is read from here on.
        seq = current_sequence(registry)
        try:
            undo = registry.config.apply_patch_checked(path, patch, check=check, dry_run=dry_run)
        except ConfigPatchError as e:
            raise AgentApiError(str(e)) from None

        result = {"applied": bool(undo) and not dry_run, "changed": changed, "warnings": warnings, "seq": seq}
        if not undo:
            result["note"] = "the patch changes nothing"
        elif not dry_run:
            result["undo"] = undo
            self.logger.info("config changed: %s, patch at '%s': %s", ", ".join(changed), path, json.dumps(patch))
            registry.notify_user(
                f"Agent changed {', '.join(changed)}",
                "Undo",
                # Clicked on the UI thread; applying a config may restart a source or pipeline.
                lambda: self.shared.tasks.run_task(self._undo_config_change, undo, changed),
            )
        return result

    def _undo_config_change(self, undo: list, changed: list):
        """The Undo button of a change's notice. The undo patch tests what it is about to
        replace or remove, so it is refused as a whole once the same place was edited again."""
        registry = self.shared.registry
        try:
            registry.config.apply_patch_checked("/", undo)
        except ConfigPatchError as e:
            self.logger.warning("could not undo the change to %s: %s", ", ".join(changed), e)
            registry.notify_user(f"Could not undo the change to {', '.join(changed)}: it was edited since")
            return
        self.logger.info("config change undone: %s", ", ".join(changed))
        registry.notify_user(f"Undone: {', '.join(changed)}")

    def _tool_command(self, args: dict) -> dict:
        source_id, source = self._resolve_source(args)
        command = args.get("command")
        commands = {token: label for token, label in source.get_commands()}
        if command not in commands:
            raise AgentApiError(f"unknown command '{command}' for '{source_id}', available: {sorted(commands)}")

        # Taken before the command, like send's: a reset's boot log is read from here on.
        seq = current_sequence(self.shared.registry)
        self.logger.info("command to '%s': %s", source_id, command)
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
