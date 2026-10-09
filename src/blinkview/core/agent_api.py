# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Agent API transport: a localhost HTTP server a running BlinkView hosts so an outside process
(the `blink mcp` shim, see agent_mcp.py) can act on it, plus the client side that finds running
servers. See plans/claude-live-api.md.

Standard library only, and nothing from the numba/Qt cluster: `blink mcp` imports this module
and has to start fast. What the tools actually do lives with whoever hosts the server
(parsers/mcp_server.py); this file only moves JSON.

Each server writes a run file `~/.blinkview/run/<pid>-<port>.json` holding its port and a random
token. Always under the user's home, never the project's `.blinkview/`: the token must not end
up in a repository. A request without that token is rejected."""

import hmac
import json
import os
import secrets
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from blinkview.utils.atomic_json_dump import atomic_json_dump
from blinkview.utils.global_settings import get_blink_home

API_PREFIX = "/v1/"
MAX_REQUEST_BYTES = 1 << 20
# How long serve_until() waits for a connection before re-checking its stop condition.
ACCEPT_POLL_SECONDS = 0.2
# Windows takes about 2 s to report "connection refused" on a closed localhost port; a shorter
# timeout would read a dead instance as merely slow and never clean up its run file.
PROBE_TIMEOUT_SECONDS = 4.0

ToolHandler = Callable[[dict], Any]


class AgentApiError(Exception):
    """A request that can't be carried out; the message is returned to the caller as is."""


def get_run_dir() -> Path:
    return get_blink_home() / "run"


class _Server(ThreadingHTTPServer):
    daemon_threads = True  # a request still running must not keep the app alive at exit
    allow_reuse_address = False

    handlers: Dict[str, ToolHandler]
    token: str


class _RequestHandler(BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler's signature
        pass  # stderr access log off; the host logs what each tool did

    def _reply(self, status: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's naming
        expected = f"Bearer {self.server.token}"
        if not hmac.compare_digest(self.headers.get("Authorization", "").encode(), expected.encode()):
            self._reply(401, {"ok": False, "error": "missing or wrong token"})
            return

        handler = self.server.handlers.get(self.path[len(API_PREFIX) :]) if self.path.startswith(API_PREFIX) else None
        if handler is None:
            self._reply(404, {"ok": False, "error": f"unknown tool: {self.path}"})
            return

        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_REQUEST_BYTES:
                raise AgentApiError("request too large")
            args = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(args, dict):
                raise AgentApiError("request body must be a JSON object")
        except (ValueError, AgentApiError) as e:
            self._reply(400, {"ok": False, "error": f"bad request: {e}"})
            return

        try:
            result = handler(args)
        except AgentApiError as e:
            self._reply(400, {"ok": False, "error": str(e)})
            return
        except Exception as e:
            self._reply(500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            return
        self._reply(200, {"ok": True, "result": result})


class AgentApiServer:
    """One listening socket on 127.0.0.1 and its run file. `handlers` maps a tool name to a
    function taking the request's JSON object and returning something JSON-serializable; it
    runs on a per-request thread."""

    def __init__(
        self, handlers: Dict[str, ToolHandler], port: int = 0, run_info: dict = None, run_dir: Optional[Path] = None
    ):
        self.token = secrets.token_urlsafe(32)
        self._server = _Server(("127.0.0.1", port), _RequestHandler)
        self._server.handlers = handlers
        self._server.token = self.token
        self._server.timeout = ACCEPT_POLL_SECONDS
        self.port = self._server.server_address[1]

        self.run_file = (run_dir or get_run_dir()) / f"{os.getpid()}-{self.port}.json"
        info = dict(run_info or {})
        info.update(pid=os.getpid(), port=self.port, token=self.token, started=datetime.now(timezone.utc).isoformat())
        try:
            atomic_json_dump(info, self.run_file)
        except OSError:
            self._server.server_close()
            raise

    def serve_until(self, should_stop: Callable[[], bool]):
        """Serves on the calling thread until `should_stop()` is true, then closes."""
        try:
            while not should_stop():
                self._server.handle_request()
        finally:
            self.close()

    def close(self):
        self._server.server_close()
        try:
            self.run_file.unlink()
        except OSError:
            pass


# --- Client side (used by `blink mcp`) ---


def call_instance(instance: dict, tool: str, args: Optional[dict] = None, timeout: float = 10.0) -> Any:
    """Calls one tool on the server a run file describes. Raises AgentApiError with the server's
    message when it refuses, ConnectionError when nothing answers."""
    request = urllib.request.Request(
        f"http://127.0.0.1:{instance['port']}{API_PREFIX}{tool}",
        data=json.dumps(args or {}).encode("utf-8"),
        headers={"Authorization": f"Bearer {instance['token']}", "Content-Type": "application/json"},
        method="POST",
    )
    # No proxy: a system HTTP proxy must not see (or swallow) a localhost call.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as e:
        try:
            message = json.loads(e.read()).get("error")
        except ValueError:
            message = None
        raise AgentApiError(message or f"HTTP {e.code}") from None
    except (urllib.error.URLError, OSError) as e:
        if isinstance(e, ConnectionRefusedError) or isinstance(getattr(e, "reason", None), ConnectionRefusedError):
            raise ConnectionRefusedError(f"nothing listens on port {instance['port']}") from None
        raise ConnectionError(f"BlinkView instance (pid {instance.get('pid')}) did not answer: {e}") from None
    return payload.get("result")


def list_instances(run_dir: Optional[Path] = None) -> List[dict]:
    """The run files of servers that answer, each with its live `info` under "info". A run file
    nothing listens behind (its BlinkView was killed) is deleted."""
    run_dir = run_dir or get_run_dir()
    instances = []
    for path in sorted(run_dir.glob("*.json")) if run_dir.is_dir() else []:
        try:
            instance = json.loads(path.read_text(encoding="utf-8"))
            instance["port"], instance["token"]
        except (OSError, ValueError, KeyError, TypeError):
            continue
        try:
            instance["info"] = call_instance(instance, "info", timeout=PROBE_TIMEOUT_SECONDS)
        except ConnectionRefusedError:
            try:
                path.unlink()
            except OSError:
                pass
            continue
        except ConnectionError:
            continue  # busy or slow, not provably gone
        except AgentApiError:
            continue  # something else owns that port now; not ours to delete on a guess
        instances.append(instance)
    return instances
