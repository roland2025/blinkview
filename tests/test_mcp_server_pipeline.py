# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The `mcp_server` pipeline (parsers/mcp_server.py) in a real, started Registry: tool calls go
through the real HTTP server and the same client code `blink mcp` uses, and `send` goes out
through a real tcp_client source to a socket this test listens on."""

import json
import socket
import time
from types import SimpleNamespace

import pytest

from blinkview import agent_mcp
from blinkview.core import agent_api
from blinkview.core.agent_api import AgentApiError
from tests.fakes.real_registry import make_real_registry


def _write_profile(tmp_path, sources, pipelines):
    (tmp_path / "test_config.json").write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": sources,
                "pipelines": pipelines,
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )


def _wait_for(predicate, timeout=10.0, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail(f"timed out waiting for {what}")


@pytest.fixture
def run_dir(tmp_path, monkeypatch):
    """Run files go under tmp_path, not the developer's real ~/.blinkview/run."""
    path = tmp_path / "run"
    monkeypatch.setattr(agent_api, "get_run_dir", lambda: path)
    return path


@pytest.fixture
def listener():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    sock.settimeout(10)
    yield sock
    sock.close()


@pytest.fixture
def registry(tmp_path, run_dir, listener):
    tcp = {"enabled": True, "type": "tcp_client", "host": "127.0.0.1", "port": listener.getsockname()[1]}
    _write_profile(
        tmp_path,
        sources={
            "src_board": dict(tcp, name="board", logging={"enabled": False}),
            "src_other": dict(tcp, name="other", enabled=False, logging={"enabled": False}),
        },
        pipelines={
            "pipe_board": {"enabled": True, "type": "default", "name": "board", "sources_": ["src_board"]},
            "pipe_mcp": {
                "enabled": True,
                "type": "mcp_server",
                "name": "mcp",
                "send_to_any": False,
                "send_to": ["src_board"],
            },
        },
    )
    reg = make_real_registry(tmp_path, "mcp_server_test", start=True)
    yield reg
    reg.stop()


def _call(run_dir, tool, **args):
    return agent_mcp.call_tool(tool, args, run_dir=run_dir)


def _wait_for_server(run_dir):
    return _wait_for(lambda: agent_api.list_instances(run_dir), what="the mcp_server run file")


def test_status_lists_session_and_only_the_allowed_sources(registry, run_dir):
    _wait_for_server(run_dir)

    (instance,) = _call(run_dir, "status")["instances"]

    assert instance["profile"] == registry.file_manager.profile_name
    assert instance["session_id"] == registry.file_manager.session_dir.name
    assert instance["can_rotate"] is True
    assert [(s["id"], s["name"], s["type"], s["can_send"]) for s in instance["sources"]] == [
        ("src_board", "board", "tcp_client", True)
    ]


def test_send_reaches_the_device_through_the_real_source(registry, run_dir, listener):
    _wait_for_server(run_dir)
    conn, _addr = listener.accept()
    conn.settimeout(10)
    try:
        _wait_for(lambda: registry.sources.get("src_board").client_sock, what="the tcp_client to see its connection")

        result = _call(run_dir, "send", text="status")  # the only allowed source: no 'source' needed

        assert conn.recv(100) == b"status\n"
        assert result["source"] == "src_board"

        _call(run_dir, "send", text="raw", source="board", newline=False)
        assert conn.recv(100) == b"raw"
    finally:
        conn.close()


def test_send_to_a_source_outside_send_to_is_refused(registry, run_dir):
    _wait_for_server(run_dir)
    with pytest.raises(AgentApiError, match="unknown or not allowed source 'src_other'"):
        _call(run_dir, "send", text="x", source="src_other")
    with pytest.raises(AgentApiError, match="'text' must be"):
        _call(run_dir, "send", text="")


def test_by_default_every_source_is_allowed(registry, run_dir):
    """`send_to_any` defaults to on: without it in the config, `send_to` is not consulted."""
    _wait_for_server(run_dir)
    registry.config.apply_patch("/pipelines/pipe_mcp", [{"op": "remove", "path": "/send_to_any"}])

    instance = _wait_for(
        lambda: next((i for i in agent_api.list_instances(run_dir) if len(i["info"]["sources"]) == 2), None),
        what="the restarted server to list both sources",
    )

    assert [s["id"] for s in instance["info"]["sources"]] == ["src_board", "src_other"]
    with pytest.raises(AgentApiError, match="'source' is required, one of: \\['board', 'other'\\]"):
        _call(run_dir, "send", text="x")
    assert _call(run_dir, "send", text="x", source="other")["source"] == "src_other"


def test_command_must_be_one_the_source_offers(registry, run_dir):
    _wait_for_server(run_dir)
    with pytest.raises(AgentApiError, match="unknown command 'reset'"):
        _call(run_dir, "command", command="reset")

    source = registry.sources.get("src_board")
    received = []
    source.get_commands = lambda: [("reset", "Reset MCU")]
    source.send_command = received.append

    seq_before = _call(run_dir, "status")["instances"][0]["seq"]
    result = _call(run_dir, "command", command="reset")
    assert received == ["reset"]
    assert (result["source"], result["command"]) == ("src_board", "reset")
    # The log cursor from just before the command (startup lines may still be arriving).
    assert seq_before <= result["seq"] <= _call(run_dir, "status")["instances"][0]["seq"]


def test_rotate_session_starts_a_new_session_folder(registry, run_dir):
    _wait_for_server(run_dir)
    old_dir = registry.file_manager.session_dir

    result = _call(run_dir, "rotate_session", name="after flash")

    new_dir = registry.file_manager.session_dir
    assert new_dir != old_dir
    assert result["old_session_id"] == old_dir.name
    assert result["session_id"] == new_dir.name
    assert registry.file_manager.session_display_name == "after_flash"  # sanitized for the folder name
    # The server keeps running across the rotation, and reports the new session.
    assert _call(run_dir, "status")["instances"][0]["session_id"] == new_dir.name


def test_rotate_session_goes_through_the_gui_handler_when_one_is_installed(registry, run_dir):
    _wait_for_server(run_dir)
    fm = registry.file_manager
    asked = []

    def handler(display_name):
        asked.append(display_name)
        return fm.session_dir, fm.session_dir

    registry.session_rotation_handler = handler
    _call(run_dir, "rotate_session")
    assert asked == [None]


def test_allow_rotate_off_and_server_stops_with_the_pipeline(registry, run_dir):
    _wait_for_server(run_dir)
    registry.config.apply_patch("/pipelines/pipe_mcp", [{"op": "add", "path": "/allow_rotate", "value": False}])
    # A config change restarts the pipeline's thread, so the server comes back on a new port.
    _wait_for(
        lambda: [i for i in agent_api.list_instances(run_dir) if i["info"]["can_rotate"] is False],
        what="the restarted server",
    )
    with pytest.raises(AgentApiError, match="turned off"):
        _call(run_dir, "rotate_session")

    registry.config.apply_patch("/pipelines/pipe_mcp", [{"op": "replace", "path": "/enabled", "value": False}])
    _wait_for(lambda: not list(run_dir.glob("*.json")), what="the run file to be removed")


# ---------------------------------------------------------------------------
# read_logs, and send's reply
# ---------------------------------------------------------------------------


@pytest.fixture
def device(registry, run_dir, listener):
    """The other end of the board's connection, with the parser's framing already synced: the
    first line a source receives is dropped (see FrameDecoder.frame_resync_on_start)."""
    _wait_for_server(run_dir)
    conn, _addr = listener.accept()
    conn.settimeout(10)
    _wait_for(lambda: registry.sources.get("src_board").client_sock, what="the tcp_client to see its connection")
    conn.sendall(b"priming line" + NL)
    yield conn
    conn.close()


NL = bytes([10])


def _say(conn, *lines):
    conn.sendall(b"".join(line.encode() + NL for line in lines))


def _messages(result):
    """The message part of each returned line (the formatter's columns stripped)."""
    return [line.rsplit(": ", 1)[-1] for line in result["text"].splitlines()]


def test_read_logs_returns_the_newest_matching_lines(registry, run_dir, device):
    _say(device, "alpha one", "beta two", "alpha three")

    result = _wait_for(
        lambda: (r := _call(run_dir, "read_logs", device="board", contains="alpha")) and r["lines"] == 2 and r,
        what="both alpha lines",
    )

    assert _messages(result) == ["alpha one", "alpha three"]
    assert result["next_seq"] > 0
    assert result["session_id"] == registry.file_manager.session_dir.name
    assert _messages(_call(run_dir, "read_logs", device="board", last_n=1)) == ["alpha three"]


def test_cursor_returns_only_what_is_new_and_waits_for_it(registry, run_dir, device):
    seq = _call(run_dir, "status")["instances"][0]["seq"]
    _say(device, "first", "second")

    result = _call(run_dir, "read_logs", after_seq=seq, device="board", wait_s=10, quiet_ms=300)

    assert _messages(result) == ["first", "second"]
    assert result["stopped_on"] == "quiet"
    assert _call(run_dir, "read_logs", after_seq=result["next_seq"], device="board")["lines"] == 0

    _say(device, "third")
    again = _call(run_dir, "read_logs", after_seq=result["next_seq"], device="board", wait_s=10)
    assert _messages(again) == ["third"]
    assert again["stopped_on"] == "first_lines"


def test_wait_times_out_when_nothing_matches(registry, run_dir, device):
    started = time.monotonic()
    result = _call(run_dir, "read_logs", device="board", wait_s=0.5, until="never printed")
    assert result["stopped_on"] == "timeout"
    assert result["lines"] == 0
    assert 0.4 < time.monotonic() - started < 5


def test_more_new_lines_than_max_lines_pages_forward_without_skipping(registry, run_dir, device):
    seq = _call(run_dir, "status")["instances"][0]["seq"]
    _say(device, *[f"row {i:02d}" for i in range(25)])
    _wait_for(lambda: _call(run_dir, "read_logs", device="board", contains="row 24")["lines"], what="all rows")

    seen = []
    cursor = seq
    for _page in range(3):
        result = _call(run_dir, "read_logs", after_seq=cursor, device="board", max_lines=10)
        seen.append((_messages(result), result["truncated"]))
        cursor = result["next_seq"]

    assert seen == [
        ([f"row {i:02d}" for i in range(0, 10)], True),
        ([f"row {i:02d}" for i in range(10, 20)], True),
        ([f"row {i:02d}" for i in range(20, 25)], False),
    ]


def test_send_with_wait_returns_the_devices_reply(registry, run_dir, device):
    import threading

    def firmware():
        assert device.recv(100) == b"ver" + NL
        _say(device, "version 1.2", "OK")

    thread = threading.Thread(target=firmware, daemon=True)
    thread.start()

    result = _call(run_dir, "send", text="ver", wait_s=10, until="OK", device="board")

    thread.join(5)
    assert _messages(result["reply"]) == ["version 1.2", "OK"]
    assert result["reply"]["stopped_on"] == "until"
    # The cursor returned by send marks the same point for a later read.
    assert _messages(_call(run_dir, "read_logs", after_seq=result["seq"], device="board")) == ["version 1.2", "OK"]


def test_blinkviews_own_lines_are_hidden_unless_asked_for(registry, run_dir, device):
    seq = _call(run_dir, "status")["instances"][0]["seq"]
    _call(run_dir, "send", text="ping")  # logged by the pipeline as a system line
    assert device.recv(100) == b"ping" + NL
    _say(device, "pong")
    _wait_for(lambda: _call(run_dir, "read_logs", after_seq=seq, contains="pong")["lines"], what="the reply")

    hidden = _call(run_dir, "read_logs", after_seq=seq)
    shown = _call(run_dir, "read_logs", after_seq=seq, include_system=True)

    assert _messages(hidden) == ["pong"]
    assert "send to 'src_board'" in shown["text"]
    assert "send to 'src_board'" in _call(run_dir, "read_logs", after_seq=seq, device="system")["text"]


def test_formatter_options_are_the_log_windows(registry, run_dir, device):
    _say(device, "just this")
    _wait_for(lambda: _call(run_dir, "read_logs", device="board", contains="just this")["lines"], what="the line")

    bare = _call(
        run_dir, "read_logs", device="board", last_n=1, show_ts=False, show_dev=False, show_lvl=False, show_mod=False
    )
    full = _call(run_dir, "read_logs", device="board", last_n=1, ts_precision=6, utc=True)

    assert bare["text"].strip() == "just this"
    assert full["utc_offset_minutes"] == 0
    assert "board" in full["text"] and full["text"].rstrip().endswith("just this")
    assert len(full["text"]) > len(_call(run_dir, "read_logs", device="board", last_n=1, utc=True)["text"])


def test_read_logs_refuses_bad_filters_without_registering_anything(registry, run_dir, device):
    devices_before = len(registry.id_registry.get_all_devices())
    with pytest.raises(AgentApiError, match="unknown device 'bord'"):
        _call(run_dir, "read_logs", device="bord")
    with pytest.raises(AgentApiError, match="'module' needs 'device'"):
        _call(run_dir, "read_logs", module="log")
    with pytest.raises(AgentApiError, match="has no module 'nope'"):
        _call(run_dir, "read_logs", device="board", module="nope")
    with pytest.raises(AgentApiError, match="unknown 'min_level'"):
        _call(run_dir, "read_logs", min_level="LOUD")
    assert len(registry.id_registry.get_all_devices()) == devices_before


def test_allow_read_off(registry, run_dir):
    _wait_for_server(run_dir)
    registry.config.apply_patch("/pipelines/pipe_mcp", [{"op": "add", "path": "/allow_read", "value": False}])
    _wait_for(
        lambda: [i for i in agent_api.list_instances(run_dir) if i["info"]["can_read"] is False],
        what="the restarted server",
    )
    with pytest.raises(AgentApiError, match="reading logs is turned off"):
        _call(run_dir, "read_logs")
    with pytest.raises(AgentApiError, match="reading logs is turned off"):
        _call(run_dir, "get_config")
    with pytest.raises(AgentApiError, match="reading logs is turned off"):
        _call(run_dir, "summary")


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def _push_levels(registry, rows, device_name="rig"):
    """Rows (module path, level or None, message) straight into central storage, 2 ms apart on
    whole milliseconds. Returns each row's timestamp as a returned line would show it in UTC."""
    from blinkview.core.agent_log_read import _format_ts
    from blinkview.core.numpy_batch_manager import PooledLogBatch

    rig = registry.id_registry.get_device(device_name)
    module_ids = [rig.get_module(module_path).id for module_path, _level, _message in rows]

    # These rows skip the reorder stage. BlinkView's own lines (startup, and the registrations
    # just above) must be through it first: arriving after these with earlier timestamps, they
    # would leave the segment out of time order.
    log_pool = registry.central.log_pool
    quiet_since, last_seq = time.monotonic(), log_pool.latest_sequence()
    while time.monotonic() - quiet_since < 0.4:
        time.sleep(0.05)
        if log_pool.latest_sequence() != last_seq:
            quiet_since, last_seq = time.monotonic(), log_pool.latest_sequence()

    batch = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(rows), 8192, has_levels=True, has_modules=True, has_devices=True
    )
    stamps = []
    with batch:
        ts = (registry.now_ns() // 1_000_000 + 1) * 1_000_000
        for (_module_path, level, message), module_id in zip(rows, module_ids):
            batch.insert_any(
                ts,
                ts,
                message.encode(),
                level=level.value if level is not None else 0,
                module=module_id,
                device=rig.id,
            )
            stamps.append(_format_ts(ts, 0))
            ts += 2_000_000
        registry.central.log_pool.batch_append(batch)
    return stamps


def test_summary_counts_rows_per_module_and_level(registry, run_dir):
    from blinkview.utils.log_level import LogLevel

    _wait_for_server(run_dir)
    stamps = _push_levels(
        registry,
        [
            ("battery", LogLevel.ERROR, "pack fault"),
            ("battery", LogLevel.INFO, "pack ok"),
            ("motor", LogLevel.WARN, "hot"),
            ("battery", LogLevel.INFO, "pack ok"),
            ("battery.vbat", None, "3.71"),
            ("motor", LogLevel.INFO, "idle"),
        ],
    )

    result = _call(run_dir, "summary", device="rig", utc=True)

    assert result["rows"] == 6
    assert result["levels"] == {"ERROR": 1, "WARNING": 1, "INFO": 3, "NONE": 1}
    assert list(result["levels"]) == ["ERROR", "WARNING", "INFO", "NONE"]  # most severe first
    assert result["text"] == "3 rig battery ERROR=1 INFO=2\n2 rig motor WARNING=1 INFO=1\n1 rig battery.vbat NONE=1\n"
    assert (result["modules"], result["modules_listed"], result["truncated"]) == (3, 3, False)
    assert (result["first_time"], result["last_time"]) == (stamps[0], stamps[-1])
    assert result["session_id"] == registry.file_manager.session_dir.name

    # read_logs' filters.
    problems = _call(run_dir, "summary", device="rig", min_level="WARN")
    assert problems["text"] == "1 rig battery ERROR=1\n1 rig motor WARNING=1\n"
    assert _call(run_dir, "summary", device="rig", contains="fault")["rows"] == 1
    capped = _call(run_dir, "summary", device="rig", module="battery", max_modules=1)
    assert capped["text"] == "3 rig battery ERROR=1 INFO=2\n"
    assert (capped["rows"], capped["modules"], capped["modules_listed"], capped["truncated"]) == (4, 2, 1, True)
    # BlinkView's own lines are left out unless asked for.
    assert "system" not in _call(run_dir, "summary")["text"]
    assert " system " in _call(run_dir, "summary", include_system=True)["text"]

    # A time range: start and end are both inclusive.
    assert _call(run_dir, "summary", device="rig", utc=True, start=stamps[3])["rows"] == 3
    assert _call(run_dir, "summary", device="rig", utc=True, end=stamps[1])["rows"] == 2
    window = _call(run_dir, "summary", device="rig", utc=True, start=stamps[1], end=stamps[2])
    assert window["text"] == "1 rig battery INFO=1\n1 rig motor WARNING=1\n"
    assert (window["first_time"], window["last_time"]) == (stamps[1], stamps[2])
    assert _call(run_dir, "summary", device="rig", since_s=3600)["rows"] == 6
    nothing = _call(run_dir, "summary", device="rig", start="2099-01-01 00:00:00")
    assert (nothing["rows"], nothing["text"], nothing["levels"]) == (0, "", {})
    assert "first_time" not in nothing

    # The cursor: only what was logged after an earlier summary.
    _push_levels(registry, [("motor", LogLevel.ERROR, "stalled")])
    since = _call(run_dir, "summary", device="rig", after_seq=result["next_seq"])
    assert since["text"] == "1 rig motor ERROR=1\n"
    assert _call(run_dir, "summary", device="rig", after_seq=since["next_seq"])["rows"] == 0
    assert _messages(_call(run_dir, "read_logs", device="rig", after_seq=result["next_seq"])) == ["stalled"]


def test_summary_refuses_bad_requests(registry, run_dir):
    _wait_for_server(run_dir)

    with pytest.raises(AgentApiError, match="give one of 'after_seq', 'since_s' and 'start'"):
        _call(run_dir, "summary", since_s=10, after_seq=1)
    with pytest.raises(AgentApiError, match="cannot read 'start' time 'yesterday'"):
        _call(run_dir, "summary", start="yesterday")
    with pytest.raises(AgentApiError, match="cannot read 'end' time"):
        _call(run_dir, "summary", end="later")
    with pytest.raises(AgentApiError, match="unknown device 'nope'"):
        _call(run_dir, "summary", device="nope")


# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------


def test_get_config_returns_the_profile_at_a_path(registry, run_dir):
    _wait_for_server(run_dir)

    assert _call(run_dir, "get_config", path="/pipelines/pipe_mcp/send_to") == {
        "path": "/pipelines/pipe_mcp/send_to",
        "config": ["src_board"],
    }
    assert _call(run_dir, "get_config", path="/pipelines/pipe_mcp/send_to/0")["config"] == "src_board"
    assert _call(run_dir, "get_config", path="/sources/src_board")["config"]["host"] == "127.0.0.1"

    whole = _call(run_dir, "get_config")
    assert whole["path"] == "/"
    assert whole["config"] == registry.config.get_full_config()

    outline = _call(run_dir, "get_config", depth=2)["config"]
    assert outline["sources"] == {"src_board": {}, "src_other": {}}
    assert outline["version"] == "0.2"


def test_get_config_refuses_bad_requests(registry, run_dir, monkeypatch):
    from blinkview.parsers import mcp_server

    _wait_for_server(run_dir)

    with pytest.raises(AgentApiError, match="nothing at '/sources/nope'"):
        _call(run_dir, "get_config", path="/sources/nope")
    with pytest.raises(AgentApiError, match="'path' must start with '/'"):
        _call(run_dir, "get_config", path="sources")
    for depth in (0, True, "2"):
        with pytest.raises(AgentApiError, match="'depth' must be"):
            _call(run_dir, "get_config", depth=depth)

    monkeypatch.setattr(mcp_server, "MAX_CONFIG_CHARS", 100)
    with pytest.raises(AgentApiError, match=r"is \d+ characters \(max 100\): pass 'depth'"):
        _call(run_dir, "get_config")
    assert _call(run_dir, "get_config", depth=1)["config"]["sources"] == {}


# ---------------------------------------------------------------------------
# list_types, get_schema, apply_config_patch
# ---------------------------------------------------------------------------


def test_list_types_and_get_schema_describe_what_the_config_takes(registry, run_dir):
    _wait_for_server(run_dir)

    overview = _call(run_dir, "list_types")
    assert overview["config_paths"] == {
        "/sources/<id>": "source",
        "/pipelines/<id>": "parser",
        "/central": "central",
        "/reorder": "reorder",
    }
    assert "anchor_word" in overview["categories"]["key_value_rule"]
    assert "tcp_client" in overview["categories"]["source"]

    rules = _call(run_dir, "list_types", category="KEY_VALUE_RULE")
    assert rules["category"] == "key_value_rule"
    assert set(rules["types"]) == set(overview["categories"]["key_value_rule"])
    assert rules["types"]["anchor_word"] == "Extract values matching startswith/endswith/contains patterns."

    by_type = _call(run_dir, "get_schema", category="key_value_rule", type="anchor_word")["schema"]
    assert by_type["properties"]["match"]["enum"] == ["starts_with", "contains", "ends_with"]
    assert "ui_order" not in by_type["properties"]["match"]

    by_path = _call(run_dir, "get_schema", path="/pipelines/pipe_mcp")
    assert by_path["path"] == "/pipelines/pipe_mcp"
    assert by_path["schema"]["properties"]["allow_config"]["default"] is False
    assert _call(run_dir, "get_schema", path="/sources/src_board/port")["schema"]["type"] == "integer"

    for args, message in [
        ({}, "pass 'path', or a 'category'"),
        ({"category": "nope", "type": "x"}, "pass 'path', or a 'category'"),
        ({"category": "source"}, "unknown type 'None' in 'source'"),
        ({"category": "source", "type": "nope"}, "unknown type 'nope' in 'source'"),
        ({"path": "/sources", "category": "source"}, "either 'path' or 'category' and 'type'"),
        ({"path": "sources"}, "'path' must start with '/'"),
        ({"path": "/sources/nope"}, "nothing at '/sources/nope'"),
        ({"path": "/version"}, "no schema for '/version'"),
    ]:
        with pytest.raises(AgentApiError, match=message):
            _call(run_dir, "get_schema", **args)
    with pytest.raises(AgentApiError, match="unknown category 'nope'"):
        _call(run_dir, "list_types", category="nope")


@pytest.fixture
def configurable(registry, run_dir):
    """The registry with `allow_config` turned on (it is off unless the user turns it on), and
    the notices it would show the user collected instead."""
    _wait_for_server(run_dir)
    assert _call(run_dir, "status")["instances"][0]["can_configure"] is False
    with pytest.raises(AgentApiError, match="changing the configuration is turned off"):
        _call(run_dir, "apply_config_patch", patch=[{"op": "remove", "path": "/sources/src_other"}])

    registry.config.apply_patch("/pipelines/pipe_mcp", [{"op": "add", "path": "/allow_config", "value": True}])
    _wait_for(
        lambda: [i for i in agent_api.list_instances(run_dir) if i["info"]["can_configure"] is True],
        what="the restarted server",
    )
    notices = []
    registry.user_notice_handler = lambda message, action_text, action: notices.append((message, action_text, action))
    return notices


def _profile_on_disk(registry):
    return json.loads(registry.config.filepath.read_text())


def _rename_other(run_dir, name):
    patch = [{"op": "replace", "path": "/sources/src_other/name", "value": name}]
    return _call(run_dir, "apply_config_patch", patch=patch)


def test_apply_config_patch_changes_the_running_source_and_the_profile_file(registry, run_dir, configurable):
    before = registry.config.get_full_config()

    result = _call(
        run_dir,
        "apply_config_patch",
        path="/sources/src_other",
        patch=[{"op": "replace", "path": "/name", "value": "spare"}],
    )

    assert result["applied"] is True
    assert result["changed"] == ["/sources/src_other"]
    assert result["warnings"] == []
    assert isinstance(result["seq"], int)
    assert registry.sources.get("src_other").name == "spare"
    assert _profile_on_disk(registry)["sources"]["src_other"]["name"] == "spare"
    logged = _wait_for(
        lambda: _call(run_dir, "read_logs", include_system=True, contains="config changed: ")["text"],
        what="the change in the log",
    )
    assert "/sources/src_other, patch at '/sources/src_other'" in logged

    # The agent's own way back: the returned patch, from the root.
    assert _call(run_dir, "apply_config_patch", patch=result["undo"])["applied"] is True
    assert registry.config.get_full_config() == before
    assert _profile_on_disk(registry) == before
    assert registry.sources.get("src_other").name == "other"


def test_the_user_is_told_of_each_change_and_can_undo_it(registry, run_dir, configurable):
    before = registry.config.get_full_config()

    _rename_other(run_dir, "spare")

    assert [(message, action_text) for message, action_text, _ in configurable] == [
        ("Agent changed /sources/src_other", "Undo")
    ]
    configurable.pop()[2]()  # the Undo button
    _wait_for(lambda: configurable, what="the notice that it was undone")
    assert configurable[0][:2] == ("Undone: /sources/src_other", None)
    assert registry.config.get_full_config() == before
    assert registry.sources.get("src_other").name == "other"

    # Undo of a change that was edited over since: refused, and the later edit stays.
    _rename_other(run_dir, "one")
    undo_first = configurable.pop()[2]
    _rename_other(run_dir, "two")
    configurable.clear()
    undo_first()
    _wait_for(lambda: configurable, what="the notice that it could not be undone")
    assert configurable[0][0].startswith("Could not undo the change to /sources/src_other")
    assert registry.sources.get("src_other").name == "two"


def test_apply_config_patch_refuses_what_would_break_the_config(registry, run_dir, configurable):
    before, before_file = registry.config.get_full_config(), registry.config.filepath.read_text()

    for patch, message in [
        ([{"op": "replace", "path": "/sources/src_other/port", "value": "80"}], "port: expected integer, got string"),
        ([{"op": "add", "path": "/sources/src_new", "value": {"type": "tcp"}}], "/sources/src_new: unknown type 'tcp'"),
        ([{"op": "remove", "path": "/sources/src_board"}], "sources_/0: no entry 'src_board' in /sources"),
        ([{"op": "replace", "path": "/version", "value": "9"}], "'/version' cannot be changed here"),
        ([{"op": "add", "path": "/pipelines/pipe_mcp/send_to_any", "value": True}], "is a 'mcp_server' pipeline"),
        ([{"op": "replace", "path": "/sources/src_gone/name", "value": "x"}], "the patch does not apply"),
        ([{"op": "replace", "path": "sources/src_other/name", "value": "x"}], "must be '' or start with '/'"),
        ("nope", "non-empty list"),
    ]:
        with pytest.raises(AgentApiError, match=message):
            _call(run_dir, "apply_config_patch", patch=patch)

    assert registry.config.get_full_config() == before
    assert registry.config.filepath.read_text() == before_file
    assert configurable == []


def test_apply_config_patch_dry_run_and_warnings(registry, run_dir, configurable):
    before = registry.config.get_full_config()
    patch = [{"op": "add", "path": "/sources/src_other/hots", "value": "10.0.0.1"}]

    checked = _call(run_dir, "apply_config_patch", patch=patch, dry_run=True)

    assert checked["applied"] is False
    assert checked["changed"] == ["/sources/src_other"]
    assert len(checked["warnings"]) == 1 and "unknown key 'hots'" in checked["warnings"][0]
    assert "undo" not in checked
    assert registry.config.get_full_config() == before
    assert configurable == []

    same = _rename_other(run_dir, "other")
    assert same["applied"] is False and same["note"] == "the patch changes nothing"
    assert configurable == []


def test_a_key_value_pipeline_added_by_patch_extracts_from_new_lines(registry, run_dir, device, configurable):
    """The loop the config tools exist for: add a rule, then see its rows in the log."""
    pipeline = {
        "enabled": True,
        "type": "multi_rule_key_value",
        # Its own device name: the parser skips rows of the device it writes to.
        "name": "telemetry",
        "sources_": ["pipe_board"],
        "rules": [{"type": "key_value", "module_name": "board.log"}],
    }
    patch = [{"op": "add", "path": "/pipelines/pipe_kv", "value": pipeline}]
    result = _call(run_dir, "apply_config_patch", patch=patch)
    assert result["changed"] == ["/pipelines/pipe_kv"] and result["warnings"] == []

    _say(device, "vbat=3.71 soc=80")

    def rows():
        try:  # the device does not exist before its first row
            found = _call(run_dir, "read_logs", device="telemetry", after_seq=result["seq"])
        except AgentApiError:
            return None
        return found if found["lines"] == 2 else None

    # Generous: the extraction kernel may have to compile first.
    extracted = _wait_for(rows, timeout=120, what="the extracted rows")
    assert sorted(_messages(extracted)) == ["3.71", "80"]
    assert "log.vbat: 3.71" in extracted["text"] and "log.soc: 80" in extracted["text"]
    assert _profile_on_disk(registry)["pipelines"]["pipe_kv"] == pipeline


# ---------------------------------------------------------------------------
# Watch lists: create_watch, open_view, get_config / apply_config_patch with target='gui'
# ---------------------------------------------------------------------------


@pytest.fixture
def gui(registry, run_dir, tmp_path):
    """What BlinkMainWindow gives the registry: the GUI's own config and a way to open a view
    (here a list of what was asked for). tests/test_mcp_watches_ui.py has the real window."""
    from blinkview.core.config_manager import ConfigManager

    _wait_for_server(run_dir)
    with pytest.raises(AgentApiError, match="runs without a window"):
        _call(run_dir, "create_watch", name="Battery")
    assert _call(run_dir, "status")["instances"][0]["can_edit_watches"] is False

    registry.gui_config = ConfigManager(tmp_path / "gui.json", tmp_path / "gui.autosave.json", {"watches": {}})
    opened = []
    registry.open_view_handler = lambda kind, view_id: opened.append((kind, view_id))
    notices = []
    registry.user_notice_handler = lambda message, action_text, action: notices.append((message, action_text, action))
    return SimpleNamespace(config=registry.gui_config, opened=opened, notices=notices)


def test_create_watch_writes_the_watch_and_opens_it(registry, run_dir, gui):
    registry.id_registry.get_device("board").get_module("battery.vbat")
    modules_before = len(registry.id_registry.get_all_modules())
    assert _call(run_dir, "status")["instances"][0]["can_edit_watches"] is True

    created = _call(
        run_dir,
        "create_watch",
        name=" Battery ",
        entries=[
            {"type": "section", "label": "Pack"},
            {"label": "Voltage", "modules": ["board.battery.vbat"]},
            {"label": "Current", "modules": ["board.battery.ibat"], "key": "row_mine"},
            {"type": "group_start", "label": "Actions"},
            {"type": "button", "label": "Reset", "command_payload": "reset", "target_device": "src_board"},
            {"type": "group_end"},
        ],
    )

    watch_id = created["id"]
    assert created["applied"] is True and created["opened"] is True
    assert created["changed"] == [f"/watches/{watch_id}"]
    assert created["warnings"] == [
        f"/watches/{watch_id}/entries/2: no module 'board.battery.ibat' has logged in this session"
    ]
    assert gui.opened == [("watch", watch_id)]
    assert gui.notices[0][:2] == (f"Agent changed /watches/{watch_id}", "Undo")
    # Checking the names registered nothing: a misspelled module must not appear in the sidebar.
    assert len(registry.id_registry.get_all_modules()) == modules_before

    watch = json.loads((gui.config.filepath).read_text())["watches"][watch_id]
    assert watch["id"] == watch_id and watch["name"] == "Battery"
    assert [e["type"] for e in watch["entries"]] == ["section", "row", "row", "group_start", "button", "group_end"]
    keys = [e.get("key") for e in watch["entries"]]
    assert keys[0] is None and keys[2] == "row_mine"
    assert keys[1].startswith("row_") and keys[3].startswith("grp_start_") and keys[4].startswith("btn_")
    assert len(set(keys[1:])) == 5

    assert _call(run_dir, "get_config", target="gui", path=f"/watches/{watch_id}/name")["config"] == "Battery"
    assert _call(run_dir, "open_view", id=watch_id) == {"kind": "watch", "id": watch_id, "opened": True}
    assert gui.opened == [("watch", watch_id)] * 2
    assert _call(run_dir, "create_watch", name="Unopened", open=False)["opened"] is False
    assert len(gui.opened) == 2


def test_a_watch_is_edited_by_patch_and_bad_edits_are_refused(registry, run_dir, gui):
    watch_id = _call(run_dir, "create_watch", name="Motor", entries=[{"label": "State", "modules": ["board.log"]}])[
        "id"
    ]
    path = f"/watches/{watch_id}"
    gui.config.apply_patch("/", [{"op": "add", "path": "/layout", "value": {"x": 1}}])
    before = gui.config.get_full_config()

    def patch(*ops, **args):
        return _call(run_dir, "apply_config_patch", target="gui", path=path, patch=list(ops), **args)

    for op, message in [
        (
            {"op": "add", "path": "/entries/-", "value": {"type": "gauge", "label": "x"}},
            "entries/1: unknown type 'gauge'",
        ),
        ({"op": "add", "path": "/entries/-", "value": {"type": "row", "label": "x"}}, "entries/1: a row needs 'key'"),
        ({"op": "add", "path": "/entries/-", "value": {"type": "group_end", "key": "g"}}, "group_end without"),
        ({"op": "add", "path": "/entries/-", "value": {"type": "group_start", "key": "g"}}, "without a group_end"),
        ({"op": "add", "path": "/entries/0/modules/-", "value": "nodot"}, "a list of 'device.module' names"),
        ({"op": "replace", "path": "/name", "value": ""}, "'name' must be a non-empty string"),
        ({"op": "replace", "path": "/id", "value": "other"}, "'id' must be the watch's key"),
        ({"op": "replace", "path": "/entries/9/label", "value": "x"}, "the patch does not apply"),
    ]:
        with pytest.raises(AgentApiError, match=message):
            patch(op)
    with pytest.raises(AgentApiError, match="'/layout' cannot be changed here, only /watches"):
        _call(run_dir, "apply_config_patch", target="gui", patch=[{"op": "remove", "path": "/layout"}])
    with pytest.raises(AgentApiError, match="'target' must be 'profile' or 'gui'"):
        _call(run_dir, "apply_config_patch", target="watches", patch=[{"op": "remove", "path": "/layout"}])
    assert gui.config.get_full_config() == before

    row = {"type": "row", "key": "row_speed", "label": "Speed", "modules": ["board.log"], "colour": "red"}
    result = patch({"op": "add", "path": "/entries/-", "value": row})
    assert result["changed"] == [path]
    assert len(result["warnings"]) == 1 and "unknown key 'colour' (a row has: " in result["warnings"][0]
    assert gui.config.get_by_path(f"{path}/entries/1/label") == "Speed"
    # The profile is untouched, and stays closed to the agent: allow_config is off.
    assert "watches" not in registry.config.get_full_config()
    with pytest.raises(AgentApiError, match="changing the configuration is turned off"):
        _call(run_dir, "apply_config_patch", patch=[{"op": "remove", "path": "/sources/src_other"}])

    assert _call(run_dir, "apply_config_patch", target="gui", patch=result["undo"])["applied"] is True
    assert gui.config.get_full_config() == before


def test_allow_watches_off(registry, run_dir, gui):
    registry.config.apply_patch("/pipelines/pipe_mcp", [{"op": "add", "path": "/allow_watches", "value": False}])
    _wait_for(
        lambda: [i for i in agent_api.list_instances(run_dir) if i["info"]["can_edit_watches"] is False],
        what="the restarted server",
    )
    with pytest.raises(AgentApiError, match="changing watch lists is turned off"):
        _call(run_dir, "create_watch", name="Battery")
    with pytest.raises(AgentApiError, match="changing watch lists is turned off"):
        _call(run_dir, "apply_config_patch", target="gui", patch=[{"op": "add", "path": "/watches/w", "value": {}}])
    # Reading them is reading: allow_read.
    assert _call(run_dir, "get_config", target="gui", path="/watches")["config"] == {}
    with pytest.raises(AgentApiError, match="no watch 'nope'"):
        _call(run_dir, "open_view", id="nope")


# ---------------------------------------------------------------------------
# list_modules, and read_logs around a time
# ---------------------------------------------------------------------------


def test_list_modules_names_what_read_logs_filters_take(registry, run_dir, device):
    _say(device, "hello")
    _wait_for(lambda: _call(run_dir, "read_logs", device="board", contains="hello")["lines"], what="the line")

    listing = _call(run_dir, "list_modules")
    by_name = {d["name"]: d["modules"] for d in listing["devices"]}

    assert "system" not in by_name
    assert "log" in by_name["board"]
    assert listing["truncated"] is False
    # Every listed name is accepted as a filter.
    assert _call(run_dir, "read_logs", device="board", module="log", contains="hello")["lines"] == 1

    with_system = {d["name"]: d["modules"] for d in _call(run_dir, "list_modules", include_system=True)["devices"]}
    assert "parser.mcp" in with_system["system"]
    only = _call(run_dir, "list_modules", device="system", contains="PARSER", max_modules=1)
    assert [d["name"] for d in only["devices"]] == ["system"]
    assert only["modules_listed"] == 1 and only["truncated"] is True
    assert all("parser" in m for m in only["devices"][0]["modules"])

    with pytest.raises(AgentApiError, match="unknown device 'nope'"):
        _call(run_dir, "list_modules", device="nope")


def test_module_filter_covers_the_module_and_its_submodules(registry, run_dir, device):
    """Rows pushed straight into central storage for board/battery, board/battery.vbat and
    board/motor: `module="battery"` returns the first two."""
    from blinkview.core.numpy_batch_manager import PooledLogBatch

    board = registry.id_registry.get_device("board")
    rows = [("battery", "pack ok"), ("battery.vbat", "3.71"), ("motor", "idle")]
    batch = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(rows), 4096, has_levels=True, has_modules=True, has_devices=True
    )
    with batch:
        for module_path, message in rows:
            ts = registry.now_ns()
            batch.insert_any(
                ts, ts, message.encode(), level=0, module=board.get_module(module_path).id, device=board.id
            )
        registry.central.log_pool.batch_append(batch)

    assert _messages(_call(run_dir, "read_logs", device="board", module="battery")) == ["pack ok", "3.71"]
    assert _messages(_call(run_dir, "read_logs", device="board", module="battery.vbat")) == ["3.71"]
    assert _messages(_call(run_dir, "read_logs", device="board", module="motor")) == ["idle"]


def test_read_logs_around_a_time_returns_the_lines_before_and_after_it(registry, run_dir, device):
    _say(device, "early 1", "early 2", "early 3")
    _wait_for(lambda: _call(run_dir, "read_logs", device="board", contains="early 3")["lines"], what="early lines")
    time.sleep(0.3)
    _say(device, "the error")
    _wait_for(lambda: _call(run_dir, "read_logs", device="board", contains="the error")["lines"], what="the error")
    time.sleep(0.3)
    _say(device, "late 1", "late 2", "late 3")
    _wait_for(lambda: _call(run_dir, "read_logs", device="board", contains="late 3")["lines"], what="late lines")

    # The anchor is the error line's own timestamp, copied from a returned line the way an agent would.
    error_line = _call(run_dir, "read_logs", device="board", contains="the error", ts_precision=6)["text"].strip()
    stamp = " ".join(error_line.split(" ")[:2])

    window = _call(run_dir, "read_logs", device="board", around=stamp, before=2, after=3)

    assert _messages(window) == ["early 2", "early 3", "the error", "late 1", "late 2"]
    assert (window["before_lines"], window["after_lines"]) == (2, 3)
    assert window["older_rows_exist"] is True
    # next_seq continues right after the window.
    assert _messages(_call(run_dir, "read_logs", device="board", after_seq=window["next_seq"])) == ["late 3"]

    # Same instant given in UTC with an explicit offset, and as a time of day alone.
    utc_line = _call(run_dir, "read_logs", device="board", contains="the error", ts_precision=6, utc=True)["text"]
    utc_stamp = "T".join(utc_line.strip().split(" ")[:2]) + "Z"
    assert _messages(_call(run_dir, "read_logs", device="board", around=utc_stamp, before=0, after=1)) == ["the error"]
    time_only = stamp.split(" ")[1]
    assert _messages(_call(run_dir, "read_logs", device="board", around=time_only, before=1, after=1)) == [
        "early 3",
        "the error",
    ]


def test_read_logs_around_rejects_bad_input(registry, run_dir, device):
    with pytest.raises(AgentApiError, match="cannot read 'around' time 'yesterday'"):
        _call(run_dir, "read_logs", around="yesterday")
    with pytest.raises(AgentApiError, match="cannot be combined"):
        _call(run_dir, "read_logs", around="12:00:00", wait_s=1)


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


def _push(registry, rows, device_name="board"):
    """Rows straight into central storage: [(module_path, message), ...], 1 ms apart."""
    from blinkview.core.numpy_batch_manager import PooledLogBatch

    board = registry.id_registry.get_device(device_name)
    batch = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(rows), 8192, has_levels=True, has_modules=True, has_devices=True
    )
    with batch:
        ts = registry.now_ns() - len(rows) * 1_000_000
        for module_path, message in rows:
            ts += 1_000_000
            batch.insert_any(
                ts, ts, message.encode(), level=0, module=board.get_module(module_path).id, device=board.id
            )
        registry.central.log_pool.batch_append(batch)


def _telemetry_lines(result):
    """[(device, module, value)] from telemetry_latest's text, the time column dropped."""
    out = []
    for line in result["text"].splitlines():
        left, value = line.split(" = ", 1)
        _date, _time, dev, mod = left.split(" ")
        out.append((dev, mod, value))
    return out


def test_telemetry_latest_shows_each_modules_newest_value(registry, run_dir):
    _wait_for_server(run_dir)
    _push(registry, [("battery.vbat", "3.70"), ("battery.soc", "81"), ("battery.vbat", "3.71"), ("motor", "idle")])

    result = _wait_for(
        lambda: (r := _call(run_dir, "telemetry_latest", device="board")) and r["values"] == 3 and r,
        what="the value tracker to pick the rows up",
    )

    assert _telemetry_lines(result) == [
        ("board", "battery.soc", "81"),
        ("board", "battery.vbat", "3.71"),
        ("board", "motor", "idle"),
    ]
    assert result["truncated"] is False
    assert _telemetry_lines(_call(run_dir, "telemetry_latest", device="board", module="battery")) == [
        ("board", "battery.soc", "81"),
        ("board", "battery.vbat", "3.71"),
    ]
    assert _telemetry_lines(_call(run_dir, "telemetry_latest", contains="VBAT")) == [("board", "battery.vbat", "3.71")]
    capped = _call(run_dir, "telemetry_latest", device="board", max_values=1)
    assert capped["values"] == 1 and capped["truncated"] is True

    # BlinkView's own modules only on request.
    assert all(dev != "system" for dev, _mod, _value in _telemetry_lines(_call(run_dir, "telemetry_latest")))
    assert any(
        dev == "system" for dev, _m, _v in _telemetry_lines(_call(run_dir, "telemetry_latest", include_system=True))
    )

    # Only what changed since the previous call.
    _push(registry, [("battery.soc", "80")])
    changed = _wait_for(
        lambda: (
            (r := _call(run_dir, "telemetry_latest", device="board", changed_after_seq=result["next_seq"]))
            and r["values"]
            and r
        ),
        what="the changed value",
    )
    assert _telemetry_lines(changed) == [("board", "battery.soc", "80")]


def test_telemetry_series_returns_stats_and_samples(registry, run_dir):
    _wait_for_server(run_dir)
    _push(registry, [("battery.vbat", f"{3.5 + i / 100:.2f}") for i in range(50)] + [("motor", "idle")])

    series = _call(run_dir, "telemetry_series", device="board", module="battery.vbat", max_points=5)

    assert (series["channels"], series["samples"], series["samples_returned"]) == (1, 50, 5)
    (stats,) = series["stats"]
    # approx: the kernel's float parser may be one ulp off the exact decimal (3.99 -> 3.9899999999999998).
    assert (stats["min"], stats["max"], stats["first"], stats["last"]) == pytest.approx((3.5, 3.99, 3.5, 3.99))
    assert stats["mean"] == pytest.approx(3.745)
    lines = series["text"].splitlines()
    assert [line.split(" ")[-1] for line in lines] == ["3.5", "3.62", "3.74", "3.87", "3.99"]
    # Timestamps are real, in order, and in the log lines' layout.
    stamps = [" ".join(line.split(" ")[:2]) for line in lines]
    assert stamps == sorted(stamps) and stamps[0] == series["first_time"] and stamps[-1] == series["last_time"]
    assert stamps[0][:4] == time.strftime("%Y")

    everything = _call(run_dir, "telemetry_series", device="board", module="battery.vbat", max_points=2000)
    assert everything["samples_returned"] == 50
    assert _call(run_dir, "telemetry_series", device="board", module="battery.vbat", since_s=3600)["samples"] == 50
    assert _call(run_dir, "telemetry_series", device="board", module="battery.vbat", since_s=0)["samples"] == 0


def test_telemetry_series_with_several_channels_and_bad_requests(registry, run_dir):
    _wait_for_server(run_dir)
    _push(registry, [("imu", "1.0 2.0 3.0"), ("imu", "4.0 5.0 6.0"), ("motor", "idle")])

    series = _call(run_dir, "telemetry_series", device="board", module="imu")

    assert series["channels"] == 3
    assert [(s["first"], s["last"]) for s in series["stats"]] == [(1.0, 4.0), (2.0, 5.0), (3.0, 6.0)]
    assert [line.split(" ")[2:] for line in series["text"].splitlines()] == [["1", "2", "3"], ["4", "5", "6"]]

    with pytest.raises(AgentApiError, match="has no numeric values"):
        _call(run_dir, "telemetry_series", device="board", module="motor")
    with pytest.raises(AgentApiError, match="'device' and 'module' are required"):
        _call(run_dir, "telemetry_series", device="board")
    with pytest.raises(AgentApiError, match="has no module 'nope'"):
        _call(run_dir, "telemetry_series", device="board", module="nope")


# ---------------------------------------------------------------------------
# blinkview.client (the scripting client) and examples/reset_connect_cycles.py
# ---------------------------------------------------------------------------


@pytest.fixture
def resettable(registry, device):
    """Gives the board a `reset` command; each reset makes the "firmware" print a boot log
    ending in a numbered 'network connected' line. Returns the list of resets received."""
    source = registry.sources.get("src_board")
    resets = []

    def send_command(command):
        resets.append(command)
        _say(device, "booting", f"network connected {len(resets)}")

    source.get_commands = lambda: [("reset", "Reset MCU")]
    source.send_command = send_command
    return resets


def test_client_status_and_calls_reach_the_instance(registry, run_dir, device):
    from blinkview.client import BlinkView

    bv = BlinkView.connect()

    assert bv.status()["session_id"] == registry.file_manager.session_dir.name
    assert bv.seq() <= bv.status()["seq"]  # two calls: startup lines may land in between
    assert bv.get_config("/pipelines/pipe_mcp/type") == "mcp_server"
    assert bv.get_config("/pipelines", depth=1) == {"pipe_board": {}, "pipe_mcp": {}}
    with pytest.raises(AgentApiError, match="unknown command 'reset'"):
        bv.command("reset")


def test_client_wait_for_finds_the_line_logged_after_a_command(registry, run_dir, device, resettable):
    from blinkview.client import BlinkView

    bv = BlinkView.connect()
    _say(device, "network connected 0")  # an older boot: must not be mistaken for the new one
    _wait_for(lambda: bv.read_logs(contains="network connected 0")["lines"], what="the old line")

    seq = bv.command("reset")
    match = bv.wait_for("network connected", after_seq=seq, timeout_s=10, device="board")

    assert match.text.endswith("network connected 1")
    # The cursor is just past the match: the next wait sees only the next boot.
    assert bv.wait_for("network connected", after_seq=match.next_seq, timeout_s=0.3) is None
    bv.command("reset")
    assert bv.wait_for("network connected", after_seq=match.next_seq, timeout_s=10).text.endswith("connected 2")


def test_client_wait_for_from_now_on_and_timeout(registry, run_dir, device):
    from blinkview.client import BlinkView

    bv = BlinkView.connect()
    _say(device, "ready")
    _wait_for(lambda: bv.read_logs(contains="ready")["lines"], what="the line")

    started = time.monotonic()
    assert bv.wait_for("ready", timeout_s=0.5) is None  # already logged: not "from now on"
    assert 0.4 < time.monotonic() - started < 5


def test_reset_connect_cycles_example_reports_each_cycle(registry, run_dir, device, resettable, monkeypatch, capsys):
    import runpy
    from pathlib import Path

    script = Path(__file__).resolve().parents[1] / "examples" / "reset_connect_cycles.py"
    monkeypatch.setattr("sys.argv", [str(script), "network connected", "--cycles", "3", "--timeout", "10"])

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(script), run_name="__main__")

    out = capsys.readouterr().out
    assert exit_info.value.code == 0
    assert resettable == ["reset"] * 3
    assert [line.rsplit(": ", 1)[-1] for line in out.splitlines()[:3]] == [
        "network connected 1",
        "network connected 2",
        "network connected 3",
    ]
    assert "3 of 3 cycles reached 'network connected'" in out
