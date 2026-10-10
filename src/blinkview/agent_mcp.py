# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink mcp`: a Model Context Protocol server on stdio that forwards an agent's tool calls to
a running BlinkView (one whose profile has an enabled `mcp_server` pipeline - see
parsers/mcp_server.py and core/agent_api.py).

The protocol part is hand-written rather than built on the `mcp` SDK: four tools over
newline-delimited JSON-RPC need no dependency, and this has to start fast. Standard library
only - nothing here may pull in numba or Qt."""

import json
import sys
from pathlib import Path
from typing import Any, BinaryIO, Callable, List, Optional

from blinkview.core.agent_api import AgentApiError, call_instance, list_instances

# Newest first. The client's version is echoed back when it is one of these.
PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")

# Longer than the GUI's own rotation timeout (_SessionRotationBridge.TIMEOUT_SECONDS).
ROTATE_TIMEOUT_SECONDS = 150
# Same cap as core/agent_log_read.py's MAX_WAIT_SECONDS (not imported: that module loads numba).
MAX_WAIT_SECONDS = 60

INSTRUCTIONS = (
    "Controls a running BlinkView (embedded log viewer). Call `status` first: it lists the running "
    "instances, the current session and the sources you may send to. To run a device command and see "
    "its reply, call `send` with `wait_s` (and `quiet_ms` or `until`). `read_logs` reads the live log "
    "with a sequence cursor: pass the `next_seq` of one call as `after_seq` of the next. "
    "`telemetry_latest` gives the current value of every module, `telemetry_series` a module's numeric "
    "history. `get_config` shows how the profile's sources and pipelines are set up. For a whole "
    "recorded session use the shell command `blink export SESSION_ID` (see `blink export --help`)."
)

# Filters, waiting and formatting shared by `read_logs` and the reply half of `send`. The filters
# and the formatter are the log viewer's own (see core/agent_log_read.py).
_WAIT_PROPERTIES = {
    "wait_s": {
        "type": "number",
        "description": "Wait up to this many seconds (max 60) for new lines. Alone: returns as soon as "
        "any matching line arrives.",
    },
    "until": {"type": "string", "description": "With wait_s: stop once a new line contains this text."},
    "quiet_ms": {
        "type": "integer",
        "description": "With wait_s: after the first new line, stop once nothing new arrived for this "
        "long - 'the device finished printing'. 200-500 suits a shell reply.",
    },
}
_FILTER_PROPERTIES = {
    "device": {"type": "string", "description": "Only this device's lines (a device name as shown in the lines)."},
    "module": {"type": "string", "description": "Only this module and its submodules; needs 'device'."},
    "min_level": {"type": "string", "enum": ["ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "CRITICAL"]},
    "contains": {"type": "string", "description": "Text search over device, module and message."},
    "kv": {"type": "string", "description": "key=value conditions, all must match, e.g. `state=ok id=42`."},
    "include_system": {
        "type": "boolean",
        "description": "Also return BlinkView's own lines (device 'system'), hidden by default.",
    },
    "max_lines": {"type": "integer", "description": "Default 200, max 2000."},
}
_FORMAT_PROPERTIES = {
    "show_ts": {"type": "boolean", "description": "Timestamp column (default true)."},
    "show_date": {"type": "boolean", "description": "Date in the timestamp (default true)."},
    "ts_precision": {"type": "integer", "description": "Timestamp decimals: 0, 3 (default), 6 or 9."},
    "show_rx_ts": {"type": "boolean", "description": "Also the PC receive time (default false)."},
    "show_dev": {"type": "boolean", "description": "Device column (default true)."},
    "show_lvl": {"type": "boolean", "description": "Level column (default true)."},
    "show_mod": {"type": "boolean", "description": "Module column (default true)."},
    "utc": {"type": "boolean", "description": "Timestamps in UTC instead of local time (default false)."},
}

_INSTANCE_PROPERTY = {
    "type": "integer",
    "description": "pid of the BlinkView instance, from `status`. Only needed when several are running.",
}
_SOURCE_PROPERTY = {
    "type": "string",
    "description": "Source id or name, from `status`. May be omitted when exactly one source is allowed.",
}

TOOLS = [
    {
        "name": "status",
        "description": "Lists running BlinkView instances: pid, project, profile, the current session "
        "(id, name, folder) and, per source you may send to, its id, name, type and commands.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "send",
        "description": "Sends text to a source's device (e.g. a shell command over J-Link RTT or UART), "
        "as if typed in BlinkView. The reply arrives as ordinary log lines a moment later: pass `wait_s` "
        "to get the lines logged after the send back in the same call (filter with `device` to drop "
        "other devices' chatter), or read them later with `read_logs(after_seq=<the returned seq>)`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "What to send, without the line ending."},
                "source": _SOURCE_PROPERTY,
                "newline": {"type": "boolean", "description": "Append '\\n' (default true)."},
                **_WAIT_PROPERTIES,
                **_FILTER_PROPERTIES,
                **_FORMAT_PROPERTIES,
                "instance": _INSTANCE_PROPERTY,
            },
            "required": ["text"],
        },
    },
    {
        "name": "read_logs",
        "description": "Reads log lines from the running session, formatted like BlinkView's log window. "
        "Without `after_seq`: the newest `last_n` matching lines. With `after_seq`: the lines after that "
        "cursor, oldest first. The result's `next_seq` is the cursor for the next call; `truncated` means "
        "more lines are waiting behind it. With `wait_s` it waits for new lines (from now on, unless "
        "`after_seq` is given). With `around`: the lines just before and after a point in time - what "
        "happened around an error. Device and module names for the filters come from `list_modules`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "after_seq": {"type": "integer", "description": "Cursor: `next_seq` of an earlier call, or a `seq`."},
                "last_n": {"type": "integer", "description": "Without after_seq/wait_s: how many lines (default 50)."},
                "around": {
                    "type": "string",
                    "description": "A time as the lines show it, e.g. '2026-10-09 20:52:26.508', or a time of day "
                    "alone ('20:52:26', today). Read in the lines' timezone (local, or UTC with utc=true) unless "
                    "it carries 'Z' or an offset. The result's `before_lines` tells where the anchor falls.",
                },
                "before": {"type": "integer", "description": "With around: lines before it (default 50)."},
                "after": {"type": "integer", "description": "With around: lines at or after it (default 50)."},
                **_WAIT_PROPERTIES,
                **_FILTER_PROPERTIES,
                **_FORMAT_PROPERTIES,
                "instance": _INSTANCE_PROPERTY,
            },
        },
    },
    {
        "name": "list_modules",
        "description": "Lists the devices seen in this session and the module paths under each - the names "
        "`read_logs` takes as `device` and `module`. A module filter includes its submodules "
        "(`battery` covers `battery.vbat`).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "device": {"type": "string", "description": "Only this device."},
                "contains": {"type": "string", "description": "Only module paths containing this text."},
                "include_system": {"type": "boolean", "description": "Also BlinkView's own 'system' device."},
                "max_modules": {"type": "integer", "description": "Default 500, max 5000."},
                "instance": _INSTANCE_PROPERTY,
            },
        },
    },
    {
        "name": "telemetry_latest",
        "description": "The latest value of each module - what BlinkView's telemetry table shows: one line "
        "per module, `time device module = value`, where the value is that module's most recent message "
        "(a number for extracted key/value telemetry such as `battery.vbat`, otherwise text). Use it for "
        "'what is the state right now'. Pass the result's `next_seq` as `changed_after_seq` to see only "
        "what changed since.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "device": {"type": "string", "description": "Only this device."},
                "module": {"type": "string", "description": "Only this module and its submodules; needs 'device'."},
                "contains": {"type": "string", "description": "Only module paths containing this text."},
                "changed_after_seq": {"type": "integer", "description": "Only values newer than this cursor."},
                "include_system": {"type": "boolean", "description": "Also BlinkView's own 'system' device."},
                "max_values": {"type": "integer", "description": "Default 200, max 5000."},
                "utc": {"type": "boolean", "description": "Times in UTC instead of local time (default false)."},
                "instance": _INSTANCE_PROPERTY,
            },
        },
    },
    {
        "name": "telemetry_series",
        "description": "The numeric history of one module - what a BlinkView plot draws. Returns per-channel "
        "min/max/mean/first/last over all samples in range, and up to `max_points` evenly spaced samples as "
        "lines `time value [value ...]` (a module logging several numbers per line has several channels). "
        "Fails for a module whose messages are not numeric.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "device": {"type": "string"},
                "module": {"type": "string", "description": "Exact module path, e.g. 'battery.vbat'."},
                "since_s": {"type": "number", "description": "Only the last this many seconds (default: all)."},
                "max_points": {"type": "integer", "description": "Samples returned (default 100, max 2000)."},
                "utc": {"type": "boolean", "description": "Times in UTC instead of local time (default false)."},
                "instance": _INSTANCE_PROPERTY,
            },
            "required": ["device", "module"],
        },
    },
    {
        "name": "get_config",
        "description": "Reads the running instance's profile configuration as JSON, as it is in effect "
        "now: `/sources` (the connections), `/pipelines` (parsers and their rules, by id), `/central`, "
        "`/reorder`. Start with `path='/'` and `depth=2` for an outline of what exists, then read one "
        "entry in full, e.g. `/pipelines/<id>`. Read-only.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Where to read, '/'-separated keys and list indexes (default '/', everything).",
                },
                "depth": {
                    "type": "integer",
                    "description": "Only this many levels; objects and lists below that come back empty "
                    "({} or []). Default: everything under the path.",
                },
                "instance": _INSTANCE_PROPERTY,
            },
        },
    },
    {
        "name": "command",
        "description": "Runs one of a source's own commands, as listed by `status` - for J-Link RTT: "
        "reset (reset the MCU), halt, restart (resume), rtt_restart. These act on the debug probe or "
        "port, they are not sent to the firmware. The returned `seq` is the log cursor from just before "
        "the command: `read_logs(after_seq=seq, wait_s=...)` reads what the device logged after it.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "A command token from `status`."},
                "source": _SOURCE_PROPERTY,
                "instance": _INSTANCE_PROPERTY,
            },
            "required": ["command"],
        },
    },
    {
        "name": "rotate_session",
        "description": "Ends the current recording session and starts a new, empty one; sources stay "
        "connected. Use it to get a clean log for one experiment. Returns the old and new session ids. "
        "Can take several seconds.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Display name for the new session (optional)."},
                "instance": _INSTANCE_PROPERTY,
            },
        },
    },
]


# Every tool but `status` is forwarded as is to one instance.
_INSTANCE_TOOLS = frozenset(tool["name"] for tool in TOOLS) - {"status"}


def _describe(instance: dict) -> dict:
    """An instance as `status` shows it - without its token."""
    described = {key: instance.get(key) for key in ("pid", "project", "profile", "name", "started")}
    described.update(instance.get("info") or {})
    return described


def _pick_instance(instances: List[dict], pid: Optional[int], cwd: Path) -> dict:
    if not instances:
        raise AgentApiError(
            "No BlinkView instance with an agent server is running. Start BlinkView with a profile "
            "that has an enabled pipeline of type 'mcp_server'."
        )
    if pid is not None:
        for instance in instances:
            if instance.get("pid") == pid:
                return instance
        raise AgentApiError(f"No running instance with pid {pid}; running: {[i.get('pid') for i in instances]}")
    if len(instances) == 1:
        return instances[0]

    # Several: the one whose project this agent is working in, if that settles it.
    here = [i for i in instances if i.get("project") and _is_inside(cwd, Path(i["project"]))]
    if len(here) == 1:
        return here[0]
    raise AgentApiError(
        "Several BlinkView instances are running, pass 'instance' (pid): "
        + json.dumps([_describe(i) for i in (here or instances)])
    )


def _is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except (ValueError, OSError):
        return False


def call_tool(name: str, args: dict, run_dir: Optional[Path] = None, cwd: Optional[Path] = None) -> Any:
    instances = list_instances(run_dir)
    if name == "status":
        return {"instances": [_describe(i) for i in instances]}
    if name not in _INSTANCE_TOOLS:
        raise AgentApiError(f"Unknown tool: {name}")

    args = dict(args)
    instance = _pick_instance(instances, args.pop("instance", None), cwd or Path.cwd())
    return call_instance_tool(instance, name, args)


def call_instance_tool(instance: dict, name: str, args: dict) -> Any:
    """One tool call on an already chosen instance, with an HTTP timeout that outlasts what the
    tool itself may wait for. Shared with blinkview.client."""
    timeout = ROTATE_TIMEOUT_SECONDS if name == "rotate_session" else 10.0
    wait_s = args.get("wait_s")
    if isinstance(wait_s, (int, float)) and not isinstance(wait_s, bool):
        timeout += min(max(wait_s, 0), MAX_WAIT_SECONDS)
    try:
        return call_instance(instance, name, args, timeout=timeout)
    except ConnectionError as e:
        raise AgentApiError(str(e)) from None


def _render(value: Any) -> List[dict]:
    """A tool result as MCP content: the JSON, and log text (read_logs' "text", send's
    "reply") as a block of its own so the lines arrive as lines, not as one escaped string."""
    log_text = None
    if isinstance(value, dict):
        value = dict(value)
        if isinstance(value.get("reply"), dict):
            value["reply"] = dict(value["reply"])
            log_text = value["reply"].pop("text", None)
        else:
            log_text = value.pop("text", None)

    content = [{"type": "text", "text": json.dumps(value, indent=2)}]
    if log_text:
        content.append({"type": "text", "text": log_text})
    return content


def handle_message(message: dict, tool_caller: Callable[[str, dict], Any] = call_tool) -> Optional[dict]:
    """One JSON-RPC message in, its response out (None for a notification)."""
    method = message.get("method")
    msg_id = message.get("id")
    if msg_id is None or method is None:
        return None  # a notification, or a response to something we never asked

    def result(value: dict) -> dict:
        return {"jsonrpc": "2.0", "id": msg_id, "result": value}

    if method == "initialize":
        from blinkview import __version__

        requested = (message.get("params") or {}).get("protocolVersion")
        return result(
            {
                "protocolVersion": requested if requested in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "blinkview", "version": __version__},
                "instructions": INSTRUCTIONS,
            }
        )
    if method == "ping":
        return result({})
    if method == "tools/list":
        return result({"tools": TOOLS})
    if method == "tools/call":
        params = message.get("params") or {}
        try:
            value = tool_caller(params.get("name"), params.get("arguments") or {})
            return result({"content": _render(value), "isError": False})
        except AgentApiError as e:
            return result({"content": [{"type": "text", "text": str(e)}], "isError": True})
        except Exception as e:
            return result({"content": [{"type": "text", "text": f"{type(e).__name__}: {e}"}], "isError": True})

    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}


def serve(stdin: BinaryIO, stdout: BinaryIO):
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
        else:
            response = handle_message(message) if isinstance(message, dict) else None
        if response is not None:
            stdout.write(json.dumps(response).encode("utf-8") + b"\n")
            stdout.flush()


def setup_mcp_parser(parser):
    parser.description = (
        "Runs a Model Context Protocol server on stdin/stdout that lets an AI agent act on a running "
        "BlinkView: send text and commands to sources, read log lines, start a new session. It needs a "
        "running BlinkView whose profile has an enabled pipeline of type 'mcp_server'. That pipeline's "
        "options decide what the agent may do; by default it may send to any source."
    )
    parser.epilog = "Register it with Claude Code:  claude mcp add blinkview -- blink mcp"


def run_mcp(args):
    # stdout carries the protocol and nothing else: anything printed from here on goes to stderr.
    protocol_out = sys.stdout.buffer
    sys.stdout = sys.stderr
    serve(sys.stdin.buffer, protocol_out)
