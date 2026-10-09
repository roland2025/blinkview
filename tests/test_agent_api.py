# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""core/agent_api.py (HTTP transport + instance discovery) and agent_mcp.py (the `blink mcp`
stdio protocol), without a Registry - see tests/test_mcp_server_pipeline.py for the pipeline."""

import io
import json
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from blinkview import agent_mcp
from blinkview.core.agent_api import AgentApiError, AgentApiServer, call_instance, list_instances

REPO_ROOT = Path(__file__).resolve().parents[1]


def _fail(args):
    raise AgentApiError("not allowed here")


def _crash(args):
    raise ValueError("boom")


@pytest.fixture
def server(tmp_path):
    stop = threading.Event()
    srv = AgentApiServer(
        {"info": lambda args: {"session_id": "s1"}, "echo": lambda args: args, "fail": _fail, "crash": _crash},
        run_info={"project": str(tmp_path / "proj"), "profile": "default", "name": "mcp"},
        run_dir=tmp_path / "run",
    )
    thread = threading.Thread(target=srv.serve_until, args=(stop.is_set,), daemon=True)
    thread.start()
    yield srv
    stop.set()
    thread.join(5)


def _instance(srv):
    return json.loads(srv.run_file.read_text())


class TestServer:
    def test_run_file_describes_the_server(self, server):
        instance = _instance(server)
        assert instance["pid"] == os.getpid()
        assert instance["port"] == server.port
        assert instance["profile"] == "default"
        assert len(instance["token"]) >= 32

    def test_tool_call_round_trip(self, server):
        assert call_instance(_instance(server), "echo", {"text": "héllo"}) == {"text": "héllo"}

    def test_wrong_token_is_rejected(self, server):
        instance = dict(_instance(server), token="wrong")
        with pytest.raises(AgentApiError, match="token"):
            call_instance(instance, "echo", {})

    def test_unknown_tool(self, server):
        with pytest.raises(AgentApiError, match="unknown tool"):
            call_instance(_instance(server), "nope", {})

    def test_handler_refusal_message_reaches_the_caller(self, server):
        with pytest.raises(AgentApiError, match="not allowed here"):
            call_instance(_instance(server), "fail", {})

    def test_handler_crash_is_reported_and_server_survives(self, server):
        with pytest.raises(AgentApiError, match="ValueError: boom"):
            call_instance(_instance(server), "crash", {})
        assert call_instance(_instance(server), "echo", {"a": 1}) == {"a": 1}

    def test_run_file_is_removed_when_serving_stops(self, tmp_path):
        stop = threading.Event()
        srv = AgentApiServer({}, run_dir=tmp_path / "run")
        thread = threading.Thread(target=srv.serve_until, args=(stop.is_set,), daemon=True)
        thread.start()
        assert srv.run_file.exists()
        stop.set()
        thread.join(5)
        assert not thread.is_alive()
        assert not srv.run_file.exists()


class TestDiscovery:
    def test_lists_a_live_instance_with_its_info(self, server, tmp_path):
        instances = list_instances(tmp_path / "run")
        assert [i["port"] for i in instances] == [server.port]
        assert instances[0]["info"] == {"session_id": "s1"}

    def test_stale_run_file_is_dropped_and_deleted(self, server, tmp_path):
        # A port nothing listens on: bound once to get a free number, then closed.
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            dead_port = sock.getsockname()[1]
        stale = tmp_path / "run" / f"99999-{dead_port}.json"
        stale.write_text(json.dumps({"pid": 99999, "port": dead_port, "token": "x"}))
        (tmp_path / "run" / "garbage.json").write_text("{not json")

        instances = list_instances(tmp_path / "run")

        assert [i["port"] for i in instances] == [server.port]
        assert not stale.exists()

    def test_missing_run_dir(self, tmp_path):
        assert list_instances(tmp_path / "nope") == []


class TestMcpProtocol:
    def test_initialize_echoes_a_known_protocol_version(self):
        response = agent_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}}
        )
        assert response["result"]["protocolVersion"] == "2024-11-05"
        assert response["result"]["capabilities"] == {"tools": {}}
        assert response["result"]["serverInfo"]["name"] == "blinkview"

    def test_initialize_with_an_unknown_version_offers_ours(self):
        response = agent_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2099-01-01"}}
        )
        assert response["result"]["protocolVersion"] == agent_mcp.PROTOCOL_VERSIONS[0]

    def test_notifications_get_no_response(self):
        assert agent_mcp.handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None

    def test_tools_list(self):
        response = agent_mcp.handle_message({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        assert [t["name"] for t in response["result"]["tools"]] == [
            "status",
            "send",
            "read_logs",
            "list_modules",
            "telemetry_latest",
            "telemetry_series",
            "command",
            "rotate_session",
        ]

    def test_unknown_method(self):
        response = agent_mcp.handle_message({"jsonrpc": "2.0", "id": 3, "method": "resources/list"})
        assert response["error"]["code"] == -32601

    def test_tool_error_is_a_result_with_is_error(self):
        def caller(name, args):
            raise AgentApiError("no instance")

        response = agent_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "send", "arguments": {}}}, caller
        )
        assert response["result"] == {"content": [{"type": "text", "text": "no instance"}], "isError": True}

    def test_serve_reads_lines_and_writes_one_json_per_line(self):
        stdin = io.BytesIO(
            b'{"jsonrpc":"2.0","id":1,"method":"ping"}\n'
            b"\n"
            b"not json\n"
            b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
            b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}\n'
        )
        stdout = io.BytesIO()
        agent_mcp.serve(stdin, stdout)
        responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
        assert [r["id"] for r in responses] == [1, None, 2]
        assert responses[1]["error"]["code"] == -32700


class TestMcpToolCalls:
    def test_status_hides_the_token(self, server, tmp_path):
        status = agent_mcp.call_tool("status", {}, run_dir=tmp_path / "run")
        (instance,) = status["instances"]
        assert instance["pid"] == os.getpid()
        assert instance["session_id"] == "s1"
        assert "token" not in instance

    def test_no_instance_running(self, tmp_path):
        with pytest.raises(AgentApiError, match="mcp_server"):
            agent_mcp.call_tool("send", {"text": "x"}, run_dir=tmp_path / "empty")

    def test_unknown_pid(self, server, tmp_path):
        with pytest.raises(AgentApiError, match="No running instance with pid 1"):
            agent_mcp.call_tool("send", {"text": "x", "instance": 1}, run_dir=tmp_path / "run")

    def test_several_instances_pick_the_one_of_the_current_project(self, tmp_path):
        instances = [
            {"pid": 1, "project": str(tmp_path / "a")},
            {"pid": 2, "project": str(tmp_path / "b")},
        ]
        assert agent_mcp._pick_instance(instances, None, tmp_path / "b" / "src")["pid"] == 2
        assert agent_mcp._pick_instance(instances, 1, tmp_path / "b")["pid"] == 1
        with pytest.raises(AgentApiError, match="Several BlinkView instances"):
            agent_mcp._pick_instance(instances, None, tmp_path / "elsewhere")


def test_blink_mcp_fresh_process_speaks_only_json_on_stdout(tmp_path):
    """The real `blink mcp` command: registered as a subcommand, and nothing but protocol lines
    on stdout (an MCP client treats any other output as a broken server)."""
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    env = dict(os.environ, HOME=str(fake_home), USERPROFILE=str(fake_home))

    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "status", "arguments": {}}},
    ]
    result = subprocess.run(
        [sys.executable, "-m", "blinkview", "mcp"],
        input="".join(json.dumps(r) + "\n" for r in requests).encode(),
        capture_output=True,
        cwd=str(REPO_ROOT),
        env=env,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr.decode(errors="replace")
    responses = [json.loads(line) for line in result.stdout.splitlines()]
    assert [r["id"] for r in responses] == [1, 2, 3]
    assert len(responses[1]["result"]["tools"]) == 8
    assert json.loads(responses[2]["result"]["content"][0]["text"]) == {"instances": []}
