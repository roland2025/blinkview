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

    assert _call(run_dir, "command", command="reset") == {"source": "src_board", "command": "reset"}
    assert received == ["reset"]


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
