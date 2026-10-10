# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Python client for scripts that drive a running BlinkView: the same tools `blink mcp` gives an
AI agent (see agent_mcp.py's TOOLS for their arguments and results), called directly over the
local agent API. Needs a running BlinkView whose profile has an enabled `mcp_server` pipeline.

    from blinkview.client import BlinkView

    bv = BlinkView.connect()
    seq = bv.command("reset")
    match = bv.wait_for("network connected", after_seq=seq, timeout_s=120)

Standard library only, like agent_mcp.py: importing it loads neither numba nor Qt."""

import time
from pathlib import Path
from typing import Any, NamedTuple, Optional

from blinkview.agent_mcp import MAX_WAIT_SECONDS, _pick_instance, call_instance_tool
from blinkview.core.agent_api import AgentApiError, list_instances

__all__ = ["AgentApiError", "BlinkView", "Match"]


class Match(NamedTuple):
    """What wait_for() found."""

    text: str  # the matching line, formatted like BlinkView's log window
    next_seq: int  # cursor just past it: pass as `after_seq` to look for the next one


class BlinkView:
    """One running BlinkView instance. Every method is one of the agent API's tools; a request
    the instance refuses raises AgentApiError with its message."""

    def __init__(self, instance: dict):
        self._instance = instance

    @classmethod
    def connect(cls, pid: Optional[int] = None, run_dir: Optional[Path] = None) -> "BlinkView":
        """The running instance - with several, the one with this `pid`, else the one whose
        project the script runs in."""
        return cls(_pick_instance(list_instances(run_dir), pid, Path.cwd()))

    @property
    def pid(self) -> Optional[int]:
        return self._instance.get("pid")

    def call(self, tool: str, **args) -> Any:
        """Any tool by name, for the ones without a method here."""
        return call_instance_tool(self._instance, tool, args)

    def status(self) -> dict:
        """Session, sources and their commands, and `seq`, the current log cursor."""
        return self.call("info")

    def seq(self) -> Optional[int]:
        """The current log cursor: `after_seq` for "everything from now on"."""
        return self.status()["seq"]

    def command(self, command: str, source: Optional[str] = None) -> Optional[int]:
        """Runs one of a source's own commands (`reset`, ...; see status()). Returns the log
        cursor from just before it."""
        return self.call("command", command=command, **_given(source=source))["seq"]

    def send(self, text: str, source: Optional[str] = None, **args) -> dict:
        """Sends text to a source's device. With `wait_s` (and `until` or `quiet_ms`) the
        device's answer is in the result's "reply"."""
        return self.call("send", text=text, **_given(source=source), **args)

    def read_logs(self, **args) -> dict:
        return self.call("read_logs", **args)

    def wait_for(
        self, text: str, after_seq: Optional[int] = None, timeout_s: float = 30.0, **filters
    ) -> Optional[Match]:
        """Waits for the first log line containing `text` (searched in device, module and
        message) after `after_seq` - from now on when None. Returns None when `timeout_s`
        passes without one. `filters` are read_logs' own: device, module, min_level, kv, ..."""
        deadline = time.monotonic() + timeout_s
        cursor = after_seq
        while True:
            # The server waits at most MAX_WAIT_SECONDS per call: a longer wait is several calls.
            wait_s = min(max(deadline - time.monotonic(), 0.0), MAX_WAIT_SECONDS)
            # Never 0: without both cursor and wait, read_logs returns the newest old lines.
            result = self.read_logs(
                contains=text, max_lines=1, wait_s=max(wait_s, 0.001), **_given(after_seq=cursor), **filters
            )
            if result["lines"]:
                return Match(result["text"].rstrip("\n"), result["next_seq"])
            cursor = result["next_seq"]
            if time.monotonic() >= deadline:
                return None

    def summary(self, **args) -> dict:
        """Counts of log lines per module and level; same filters as read_logs."""
        return self.call("summary", **args)

    def rotate_session(self, name: Optional[str] = None) -> dict:
        """Ends the current recording session and starts a new, empty one."""
        return self.call("rotate_session", **_given(name=name))

    def list_modules(self, **args) -> dict:
        return self.call("list_modules", **args)

    def telemetry_latest(self, **args) -> dict:
        return self.call("telemetry_latest", **args)

    def telemetry_series(self, device: str, module: str, **args) -> dict:
        return self.call("telemetry_series", device=device, module=module, **args)

    def get_config(self, path: str = "/", depth: Optional[int] = None, target: str = "profile") -> Any:
        """The profile configuration at `path` (e.g. "/pipelines"), as it is in effect now.
        `target="gui"` reads the watch lists ("/watches") instead."""
        return self.call("get_config", path=path, target=target, **_given(depth=depth))["config"]

    def list_types(self, category: Optional[str] = None) -> dict:
        """The categories and their type names, or one category's types with descriptions."""
        return self.call("list_types", **_given(category=category))

    def get_schema(
        self, path: Optional[str] = None, category: Optional[str] = None, type: Optional[str] = None
    ) -> dict:
        """The schema of the config at `path`, or of `type` in `category`."""
        return self.call("get_schema", **_given(path=path, category=category, type=type))["schema"]

    def apply_config_patch(self, patch: list, path: str = "/", dry_run: bool = False, target: str = "profile") -> dict:
        """Changes the profile configuration (or, with `target="gui"`, the watch lists) with a
        JSON Patch whose paths are relative to `path`. Refused (AgentApiError, nothing changed)
        when it would leave the config invalid. The result's "undo" is the patch that reverts
        it, applied with path "/"."""
        return self.call("apply_config_patch", patch=patch, path=path, dry_run=dry_run, target=target)

    def create_watch(self, name: str, entries: Optional[list] = None, open: bool = True) -> dict:
        """Creates a watch list and opens its window. The result's "id" names it under
        "/watches" of the GUI config."""
        return self.call("create_watch", name=name, entries=entries or [], open=open)

    def open_view(self, id: str, kind: str = "watch") -> dict:
        """Opens a view in BlinkView's window, or brings it forward."""
        return self.call("open_view", id=id, kind=kind)


def _given(**args) -> dict:
    return {key: value for key, value in args.items() if value is not None}
