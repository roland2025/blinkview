# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End to end, two real processes: `blink gui` on a profile with an `mcp_server` pipeline, and
`blink mcp` talking to it over stdio the way an agent's MCP client does. Covers what no
in-process test can: the pipeline type registering in a fresh process, instance discovery
through the run file, and `rotate_session` going through the real window's UI thread.

Isolation (fake home, BLINK_PROJECT_ROOT, offscreen) as in tests/test_blink_gui_subprocess.py."""

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import zstandard

REPO_ROOT = Path(__file__).resolve().parents[1]

STARTUP_TIMEOUT_SECONDS = 120
SHUTDOWN_TIMEOUT_SECONDS = 15


def _session_text(folder: Path) -> bytes:
    """The session's unified log; rotation compresses the finished part."""
    out = b""
    for part in sorted(folder.glob("session.*")):
        data = part.read_bytes()
        out += zstandard.ZstdDecompressor().stream_reader(data).read() if part.suffix == ".zst" else data
    return out


class _McpClient:
    def __init__(self, cwd, env):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "blinkview", "mcp"],
            cwd=str(cwd),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._next_id = 0

    def rpc(self, method, params=None):
        self._next_id += 1
        request = {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params or {}}
        self.proc.stdin.write((json.dumps(request) + "\n").encode())
        self.proc.stdin.flush()
        response = json.loads(self.proc.stdout.readline())
        assert response["id"] == self._next_id, response
        return response["result"]

    def tool(self, tool_name, **arguments):
        result = self.rpc("tools/call", {"name": tool_name, "arguments": arguments})
        return result["isError"], result["content"][0]["text"]

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(SHUTDOWN_TIMEOUT_SECONDS)


@pytest.mark.skipif(
    sys.platform == "linux",
    reason="ui/run.py forces QT_QPA_PLATFORM=xcb on Linux, which needs a display "
    "(see tests/test_blink_gui_subprocess.py).",
)
def test_agent_sends_to_a_source_and_rotates_the_session_of_a_running_blink(tmp_path):
    fake_home = tmp_path / "home"
    (fake_home / ".blinkview").mkdir(parents=True)
    (fake_home / ".blinkview" / "settings.json").write_text(json.dumps({"update": {"path": str(REPO_ROOT)}}))

    # The "device": blink's tcp_client source connects here.
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(STARTUP_TIMEOUT_SECONDS)

    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {
                    "src_board": {
                        "enabled": True,
                        "type": "tcp_client",
                        "name": "board",
                        "host": "127.0.0.1",
                        "port": listener.getsockname()[1],
                    }
                },
                "pipelines": {
                    "pipe_board": {"enabled": True, "type": "default", "name": "board", "sources_": ["src_board"]},
                    "pipe_mcp": {"enabled": True, "type": "mcp_server", "name": "mcp"},
                },
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )

    env = dict(os.environ)
    env["QT_QPA_PLATFORM"] = "offscreen"
    env["QT_API"] = "pyside6"
    env["HOME"] = str(fake_home)
    env["USERPROFILE"] = str(fake_home)
    env["BLINK_PROJECT_ROOT"] = str(tmp_path / "fake_project_root")
    env["PYTHONUNBUFFERED"] = "1"
    env.pop("NUMBA_CACHE_DIR", None)

    gui_log_path = tmp_path / "gui.log"
    conn = None
    mcp = None
    with open(gui_log_path, "w", encoding="utf-8", errors="replace") as gui_log:
        gui = subprocess.Popen(
            [sys.executable, "-u", "-m", "blinkview", "gui", "-s", "mcp_e2e", "-l", str(tmp_path / "logs")]
            + ["-c", str(config_path)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=gui_log,
            stderr=subprocess.STDOUT,
        )
        try:
            try:
                conn, _addr = listener.accept()  # the source connected: the registry has started
            except socket.timeout:
                pytest.fail(f"blink never connected its source.\n{gui_log_path.read_text(errors='replace')[-3000:]}")
            conn.settimeout(20)

            run_dir = fake_home / ".blinkview" / "run"
            deadline = time.monotonic() + 30
            while not list(run_dir.glob("*.json")):
                assert time.monotonic() < deadline, "the mcp_server pipeline never wrote its run file"
                time.sleep(0.1)

            mcp = _McpClient(tmp_path, env)
            assert mcp.rpc("initialize", {"protocolVersion": "2025-06-18"})["serverInfo"]["name"] == "blinkview"

            is_error, text = mcp.tool("status")
            assert not is_error, text
            (status,) = json.loads(text)["instances"]
            assert [s["id"] for s in status["sources"]] == ["src_board"]
            old_dir = Path(status["session_dir"])

            is_error, text = mcp.tool("send", text="hello board")
            assert not is_error, text
            assert conn.recv(100) == b"hello board\n"

            # The device answers. The first line only syncs the parser's framing (see
            # FrameDecoder.frame_resync_on_start); the second must end up in the old session.
            conn.sendall(b"priming line\nreply before rotate\n")
            time.sleep(1.0)

            # A command and its reply in one call: the reply comes back as a block of log lines.
            def firmware():
                assert conn.recv(100) == b"ver\n"
                conn.sendall(b"version 1.2\nOK\n")

            firmware_thread = threading.Thread(target=firmware, daemon=True)
            firmware_thread.start()
            sent = mcp.rpc(
                "tools/call",
                {"name": "send", "arguments": {"text": "ver", "wait_s": 20, "until": "OK", "device": "board"}},
            )
            firmware_thread.join(5)
            assert not sent["isError"], sent
            meta, reply_lines = (block["text"] for block in sent["content"])
            assert json.loads(meta)["reply"]["stopped_on"] == "until"
            assert [line.rsplit(": ", 1)[-1] for line in reply_lines.splitlines()] == ["version 1.2", "OK"]

            read = mcp.rpc("tools/call", {"name": "read_logs", "arguments": {"device": "board", "contains": "reply"}})
            assert read["content"][1]["text"].rstrip().endswith("reply before rotate")

            is_error, text = mcp.tool("rotate_session", name="second")
            assert not is_error, text
            rotated = json.loads(text)
            assert rotated["old_session_id"] == old_dir.name
            new_dir = Path(rotated["session_dir"])
            assert new_dir.is_dir() and new_dir != old_dir

            is_error, text = mcp.tool("status")
            assert json.loads(text)["instances"][0]["session_id"] == new_dir.name

            # Sources stay connected across the rotation.
            is_error, text = mcp.tool("send", text="after rotate")
            assert not is_error, text
            assert conn.recv(100) == b"after rotate\n"

            # The rotation went through the GUI: its state was saved into the old session.
            assert any("gui_state.final" in p.name for p in old_dir.iterdir())
            old_text = _session_text(old_dir)
            assert b"reply before rotate" in old_text
            assert b"send to 'src_board': 'hello board\\n'" in old_text  # what the agent did is in the log
        finally:
            if mcp is not None:
                mcp.close()
            if conn is not None:
                conn.close()
            listener.close()
            gui.terminate()
            try:
                gui.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                gui.kill()
                gui.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
