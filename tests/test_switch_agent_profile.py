# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink switch NAME --create --agent` (utils/project_settings.switch_profile): a new profile
whose only content is an `mcp_server` pipeline that may change the configuration, and a real
Registry started on that file being set up by an agent from nothing."""

import json
import socket
import time
from argparse import ArgumentParser

import pytest

from blinkview import agent_mcp
from blinkview.core import agent_api
from blinkview.core.default_config import agent_profile_config, default_profile_config
from blinkview.utils.project_settings import handle_profile_args, setup_project_parser
from tests.fakes.real_registry import make_real_registry


@pytest.fixture
def profiles(tmp_path, monkeypatch):
    root = tmp_path / "project"
    (root / ".blinkview").mkdir(parents=True)
    (root / ".blinkview" / "project.json").write_text("{}")
    monkeypatch.setenv("BLINK_PROJECT_ROOT", str(root))
    return root / ".blinkview" / "profiles"


def run_switch(*argv):
    parser = ArgumentParser()
    setup_project_parser(parser)
    handle_profile_args(parser.parse_args(list(argv)))


def _wait_for(predicate, timeout=10.0, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail(f"timed out waiting for {what}")


def test_writes_a_config_with_a_configuring_agent_pipeline(profiles, capsys):
    run_switch("board", "--create", "--agent")

    config = json.loads((profiles / "board" / "board.json").read_text())
    assert config == agent_profile_config()
    assert config["pipelines"] == {
        "pipe_mcp": {"enabled": True, "type": "mcp_server", "name": "mcp", "allow_config": True}
    }
    # Everything else is what a profile with no file starts with.
    config["pipelines"] = {}
    assert config == default_profile_config()

    assert json.loads((profiles.parent / "project.json").read_text())["active_profile"] == "board"
    out = capsys.readouterr().out
    assert "claude mcp add blinkview -- blink mcp" in out
    assert "blink -p board" in out


def test_create_without_agent_still_writes_no_config(profiles):
    run_switch("plain", "--create")

    assert list((profiles / "plain").iterdir()) == []


def test_agent_fills_an_existing_profile_that_has_no_config(profiles):
    (profiles / "old").mkdir(parents=True)
    (profiles / "old" / "old.gui_state.json").write_text("{}")

    run_switch("old", "--agent")

    assert json.loads((profiles / "old" / "old.json").read_text()) == agent_profile_config()
    assert (profiles / "old" / "old.gui_state.json").read_text() == "{}"


def test_agent_never_overwrites_an_existing_config(profiles, capsys):
    (profiles / "rtt").mkdir(parents=True)
    (profiles / "rtt" / "rtt.json").write_text('{"sources": {"keep": {}}}')

    with pytest.raises(SystemExit):
        run_switch("rtt", "--create", "--agent")

    assert (profiles / "rtt" / "rtt.json").read_text() == '{"sources": {"keep": {}}}'
    assert "already has a configuration" in capsys.readouterr().out
    # Refused before anything else: the active profile did not change either.
    assert "active_profile" not in json.loads((profiles.parent / "project.json").read_text())


def test_agent_without_create_on_a_missing_profile_writes_nothing(profiles):
    with pytest.raises(FileNotFoundError):
        run_switch("nope", "--agent")

    assert not (profiles / "nope").exists()


@pytest.mark.parametrize("name", ["bad name", "a-b", "_x", "x__y"])
def test_agent_refuses_names_filemanager_would_sanitize(profiles, name):
    """FileManager would load '<sanitized>.json' from another folder, and the agent pipeline
    would silently not be there."""
    with pytest.raises(SystemExit):
        run_switch(name, "--create", "--agent")

    assert not profiles.exists() or list(profiles.iterdir()) == []


def test_mcp_is_another_spelling_of_agent(profiles):
    run_switch("board", "--create", "--mcp")

    assert json.loads((profiles / "board" / "board.json").read_text()) == agent_profile_config()


def run_init(*argv):
    """`blink init ...` through the real entry point's own parser."""
    from blinkview import __main__ as blink_main

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sys.argv", ["blink", "init", *argv])
        blink_main.main()


BLINK_MCP = {"command": "blink", "args": ["mcp"]}


@pytest.mark.parametrize("flag", ["--agent", "--mcp"])
def test_init_agent_makes_a_project_a_profile_and_registers_the_server(tmp_path, capsys, flag):
    root = tmp_path / "fw"

    run_init(str(root), flag)

    assert "created_at" in json.loads((root / ".blinkview" / "project.json").read_text())
    # 'default' is the profile a project with no active one runs.
    profile = root / ".blinkview" / "profiles" / "default" / "default.json"
    assert json.loads(profile.read_text()) == agent_profile_config()
    assert json.loads((root / ".mcp.json").read_text()) == {"mcpServers": {"blinkview": BLINK_MCP}}
    assert "ask it to set up your board" in capsys.readouterr().out


def test_plain_init_writes_neither_profile_nor_mcp_json(tmp_path):
    root = tmp_path / "fw"

    run_init(str(root))

    assert [p.name for p in root.iterdir()] == [".blinkview"]
    assert [p.name for p in (root / ".blinkview").iterdir()] == ["project.json"]


def test_init_agent_on_an_existing_project_keeps_what_is_there(tmp_path, capsys):
    root = tmp_path / "fw"
    profile = root / ".blinkview" / "profiles" / "rtt" / "rtt.json"
    profile.parent.mkdir(parents=True)
    profile.write_text('{"sources": {"keep": {}}}')
    project = {"created_at": "2026-01-01T00:00:00+00:00", "active_profile": "rtt"}
    (root / ".blinkview" / "project.json").write_text(json.dumps(project))
    other = {"command": "npx", "args": ["other"]}
    (root / ".mcp.json").write_text(json.dumps({"mcpServers": {"other": other}, "extra": 1}))

    run_init(str(root), "--agent")

    assert json.loads((root / ".blinkview" / "project.json").read_text()) == project
    assert profile.read_text() == '{"sources": {"keep": {}}}'
    assert not (root / ".blinkview" / "profiles" / "default").exists()
    assert json.loads((root / ".mcp.json").read_text()) == {
        "mcpServers": {"other": other, "blinkview": BLINK_MCP},
        "extra": 1,
    }
    assert "already has a configuration" in capsys.readouterr().out


def test_init_agent_fills_the_active_profile_when_it_has_no_config(tmp_path):
    root = tmp_path / "fw"
    (root / ".blinkview").mkdir(parents=True)
    (root / ".blinkview" / "project.json").write_text(json.dumps({"active_profile": "can"}))

    run_init(str(root), "--agent")

    profile = root / ".blinkview" / "profiles" / "can" / "can.json"
    assert json.loads(profile.read_text()) == agent_profile_config()


def test_init_agent_leaves_its_own_and_unreadable_mcp_json_alone(tmp_path, capsys):
    mine = tmp_path / "mine"
    mine.mkdir()
    custom = {"mcpServers": {"blinkview": {"command": "uv", "args": ["run", "blink", "mcp"]}}}
    (mine / ".mcp.json").write_text(json.dumps(custom))
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / ".mcp.json").write_text("{not json")

    run_init(str(mine), "--agent")
    run_init(str(broken), "--agent")

    assert json.loads((mine / ".mcp.json").read_text()) == custom
    assert (broken / ".mcp.json").read_text() == "{not json"
    assert "claude mcp add blinkview -- blink mcp" in capsys.readouterr().out
    # The rest was still done.
    assert (broken / ".blinkview" / "profiles" / "default" / "default.json").exists()


def test_default_config_is_a_new_dict_each_call():
    first = default_profile_config()
    first["sources"]["x"] = {}

    assert default_profile_config()["sources"] == {}


def test_an_agent_sets_up_a_source_and_reads_its_log_from_the_generated_profile(profiles, tmp_path, monkeypatch):
    """The point of the flag: nothing but the generated file, and an agent that adds a source
    and its parser through the real server, then reads what the device prints."""
    run_dir = tmp_path / "run"
    monkeypatch.setattr(agent_api, "get_run_dir", lambda: run_dir)

    run_switch("board", "--create", "--agent")
    # make_real_registry pins its config to this path (never the developer's real profile).
    (tmp_path / "test_config.json").write_text((profiles / "board" / "board.json").read_text())

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)

    def call(tool, **args):
        return agent_mcp.call_tool(tool, args, run_dir=run_dir)

    registry = make_real_registry(tmp_path, "agent_profile_test", start=True)
    conn = None
    try:
        _wait_for(lambda: agent_api.list_instances(run_dir), what="the mcp_server run file")

        (instance,) = call("status")["instances"]
        assert instance["can_configure"] is True
        assert instance["sources"] == []

        result = call(
            "apply_config_patch",
            patch=[
                {
                    "op": "add",
                    "path": "/sources/src_board",
                    "value": {
                        "enabled": True,
                        "type": "tcp_client",
                        "name": "board",
                        "host": "127.0.0.1",
                        "port": listener.getsockname()[1],
                        "logging": {"enabled": False},
                    },
                },
                {
                    "op": "add",
                    "path": "/pipelines/pipe_board",
                    "value": {"enabled": True, "type": "default", "name": "board", "sources_": ["src_board"]},
                },
            ],
        )
        assert result["applied"] is True
        assert sorted(result["changed"]) == ["/pipelines/pipe_board", "/sources/src_board"]

        conn, _addr = listener.accept()
        # The first frame is dropped (FrameDecoder.frame_resync_on_start), so one goes ahead.
        conn.sendall(b"priming\nhello from the board\n")

        reply = call("read_logs", device="board", wait_s=10, until="hello from the board")
        assert "hello from the board" in reply["text"]

        # Saved to the file the registry runs on, next to the agent's own pipeline.
        saved = json.loads((tmp_path / "test_config.json").read_text())
        assert sorted(saved["pipelines"]) == ["pipe_board", "pipe_mcp"]
        assert saved["pipelines"]["pipe_mcp"]["allow_config"] is True
    finally:
        if conn is not None:
            conn.close()
        listener.close()
        registry.stop()
