# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The raw hex dump: the kernel on hand-built chunks (ops/hex_dump.py), and the `raw_hex`
pipeline (parsers/raw_hex.py) in a real, started Registry next to a source's normal parser."""

import json
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from blinkview.core.agent_log_read import read_logs
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.hex_dump import create_hex_dump_state
from blinkview.core.warmup import NumbaWarmupHelper
from blinkview.ops.hex_dump import hex_dump_line_length, nb_hex_dump
from blinkview.parsers.raw_hex import RawHexParser
from tests.fakes.real_registry import make_real_registry

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def pool(tmp_path):
    registry = make_real_registry(tmp_path, "raw_hex_kernel_test")
    yield registry.system_ctx.array_pool
    registry.stop()


def _rows(batch):
    bundle = batch.bundle
    rows = []
    for i in range(batch.size):
        start = int(bundle.offsets[i])
        text = bytes(bundle.buffer[start : start + int(bundle.lengths[i])]).decode("ascii")
        rows.append((int(bundle.rx_timestamps[i]), text))
    return rows


def _dump(pool, chunks, *, bytes_per_row=16, split_on=-1, max_bytes=0, out_rows=64, state=None):
    """Runs the kernel over `chunks` (one input row each, received at t = 1000, 2000, ...) the
    way RawHexParser.run does, flushing whenever the output fills. Returns (rows, state)."""
    state = state or create_hex_dump_state()
    state.in_idx[0] = 0
    state.in_pos[0] = 0
    rows = []
    with pool.create(PooledLogBatch, max(len(chunks), 1), sum(map(len, chunks)) + 1) as batch_in:
        for n, chunk in enumerate(chunks, start=1):
            batch_in.insert(n * 1000, n * 1000, chunk)
        while True:
            with pool.create(
                PooledLogBatch,
                out_rows,
                out_rows * hex_dump_line_length(bytes_per_row),
                has_levels=True,
                has_modules=True,
                has_devices=True,
            ) as batch_out:
                full = nb_hex_dump(
                    batch_in.bundle, state, batch_out.bundle, bytes_per_row, split_on, max_bytes, 0, 7, 3
                )
                rows.extend(_rows(batch_out))
                assert all(int(m) == 7 for m in batch_out.bundle.modules[: batch_out.size])
                assert all(int(d) == 3 for d in batch_out.bundle.devices[: batch_out.size])
            if not full:
                return rows, state


def test_a_full_row_and_a_short_one_line_up(pool):
    rows, state = _dump(pool, [b"Hello, hex world!\n\x00\xff"])

    assert [text for _, text in rows] == [
        "00000000: 48 65 6c 6c 6f 2c 20 68 65 78 20 77 6f 72 6c 64 |Hello, hex world|",
        "00000010: 21 0a 00 ff                                     |!...|",
    ]
    assert state.stream_offset[0] == 20 and state.capped[0] == 0


def test_each_chunk_starts_a_row_and_keeps_its_receive_time(pool):
    """A chunk ending mid-row is not joined with the next one: how the bytes arrived stays
    visible, and the offset column runs on across chunks."""
    rows, _ = _dump(pool, [b"abc", b"", b"defghijkl", b"m"], bytes_per_row=8)

    assert rows == [
        (1000, "00000000: 61 62 63                |abc|"),
        (3000, "00000003: 64 65 66 67 68 69 6a 6b |defghijk|"),
        (3000, "0000000b: 6c                      |l|"),
        (4000, "0000000c: 6d                      |m|"),
    ]


def test_split_on_ends_a_row_after_the_delimiter(pool):
    rows, _ = _dump(pool, [b"ab\ncd\n", b"\nefghijklm\n"], bytes_per_row=8, split_on=10)

    assert [text.split("|")[1] for _, text in rows] == ["ab.", "cd.", ".", "efghijkl", "m."]
    assert [text[:8] for _, text in rows] == ["00000000", "00000003", "00000006", "00000007", "0000000f"]
    # A delimiter that is the last byte of a full-width row ends that row, without an empty one after it.
    rows, _ = _dump(pool, [b"1234567\n89"], bytes_per_row=8, split_on=10)
    assert [text.split("|")[1] for _, text in rows] == ["1234567.", "89"]


def test_a_full_output_batch_is_continued_without_losing_or_repeating_bytes(pool):
    data = bytes(range(256)) * 3
    chunks = [data[:100], data[100:101], data[101:]]

    in_one, _ = _dump(pool, chunks, out_rows=1024)
    in_many, state = _dump(pool, chunks, out_rows=3)

    assert in_many == in_one
    assert state.stream_offset[0] == len(data)
    dumped = bytes(int(pair, 16) for _, text in in_many for pair in text[10:].split("|")[0].split())
    assert dumped == data


def test_max_bytes_stops_mid_row_and_stays_stopped(pool):
    rows, state = _dump(pool, [b"0123456789", b"abcdefghij"], bytes_per_row=8, max_bytes=12)

    assert [text.split("|")[1] for _, text in rows] == ["01234567", "89", "ab"]
    assert state.capped[0] == 1 and state.stream_offset[0] == 12

    more, state = _dump(pool, [b"more"], bytes_per_row=8, max_bytes=12, state=state)
    assert more == [] and state.stream_offset[0] == 12


def test_32_bytes_per_row(pool):
    rows, _ = _dump(pool, [bytes(range(65, 65 + 33))], bytes_per_row=32)

    assert len(rows) == 2
    assert rows[0][1].endswith("|ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`|")
    assert len(rows[0][1]) == hex_dump_line_length(32)


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------


@pytest.fixture
def listener():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    sock.settimeout(10)
    yield sock
    sock.close()


def _registry(tmp_path, listener, **hex_options):
    profile = {
        "version": "0.2",
        "sources": {
            "src_board": {
                "enabled": True,
                "type": "tcp_client",
                "host": "127.0.0.1",
                "port": listener.getsockname()[1],
                "name": "board",
                "logging": {"enabled": False},
            }
        },
        "pipelines": {
            "pipe_board": {"enabled": True, "type": "default", "name": "board", "sources_": ["src_board"]},
            "pipe_hex": {"enabled": True, "type": "raw_hex", "name": "board", "sources_": ["src_board"], **hex_options},
        },
        "plugins": {},
        "reorder": {"enabled": True, "type": "default"},
        "central": {"enabled": True, "type": "default"},
    }
    (tmp_path / "test_config.json").write_text(json.dumps(profile))
    return make_real_registry(tmp_path, "raw_hex_test", start=True)


def _wait_for(predicate, timeout=30.0, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail(f"timed out waiting for {what}")


def _lines(registry, module, expected):
    """The message part of the board's rows in `module`, once there are `expected` of them."""

    def read():
        try:  # the module does not exist before its first row
            result = read_logs(registry, {"device": "board", "module": module, "last_n": 200, "show_ts": False})
        except Exception:
            return None
        lines = [line.split(f"{module}: ", 1)[1] for line in result["text"].splitlines()]
        return lines if len(lines) >= expected else None

    return _wait_for(read, what=f"{expected} rows in {module}")


def test_a_source_feeds_its_parser_and_the_hex_dump_at_once(tmp_path, listener):
    registry = _registry(tmp_path, listener, split_on=10)
    try:
        conn, _addr = listener.accept()
        _wait_for(lambda: registry.sources.get("src_board").client_sock, what="the tcp_client to see its connection")
        # The first line a parser receives is dropped (FrameDecoder.frame_resync_on_start); the dump shows it.
        conn.sendall(b"priming line\n")
        time.sleep(0.3)
        conn.sendall(b"vbat=3.71\nsoc=80\n")

        dumped = _lines(registry, "raw.hex", 3)
        parsed = _lines(registry, "log", 2)
        conn.close()
    finally:
        registry.stop()

    assert parsed == ["vbat=3.71", "soc=80"]  # the normal parser is not disturbed
    assert [line.split("|")[1] for line in dumped] == ["priming line.", "vbat=3.71.", "soc=80."]
    assert dumped[0].startswith("00000000: 70 72 69 6d 69 6e 67 20 6c 69 6e 65 0a ")
    assert dumped[1].startswith("0000000d: ")


def test_the_dump_stops_at_max_bytes_and_says_so(tmp_path, listener):
    registry = _registry(tmp_path, listener, bytes_per_row=8, max_bytes=12)
    try:
        conn, _addr = listener.accept()
        _wait_for(lambda: registry.sources.get("src_board").client_sock, what="the tcp_client to see its connection")
        conn.sendall(b"0123456789abcdef\n")
        dumped = _lines(registry, "raw.hex", 3)
        conn.sendall(b"more bytes\n")
        time.sleep(0.5)
        after = _lines(registry, "raw.hex", 3)
        conn.close()
    finally:
        registry.stop()

    assert dumped[:2] == ["00000000: 30 31 32 33 34 35 36 37 |01234567|", "00000008: 38 39 61 62             |89ab|"]
    assert dumped[2] == "dump stopped after 12 bytes (max_bytes)"
    assert after == dumped


def test_warmup_compiles_the_shape_the_pipeline_runs(tmp_path, listener):
    """The pipeline must not compile anything a started app has not warmed up: after a real
    run, the warmup adds no specialization of the kernel (and the other way round)."""
    if not hasattr(nb_hex_dump, "signatures"):
        pytest.skip("Numba is disabled")
    registry = _registry(tmp_path, listener)
    try:
        conn, _addr = listener.accept()
        _wait_for(lambda: registry.sources.get("src_board").client_sock, what="the tcp_client to see its connection")
        conn.sendall(b"hello\n")
        _lines(registry, "raw.hex", 1)
        conn.close()
        after_run = len(nb_hex_dump.signatures)

        RawHexParser.warmup(NumbaWarmupHelper(registry.system_ctx))
    finally:
        registry.stop()

    assert after_run == len(nb_hex_dump.signatures) == 1


def test_fresh_interpreter_registers_raw_hex(tmp_path):
    """Registration is an import side effect (parsers/__init__.py): only a process that has
    imported nothing else shows whether the app would find the type."""
    script = textwrap.dedent(
        f"""
        from blinkview.core.registry import Registry
        from blinkview.core.warmup_registry import _WARMUP_CALLBACKS

        log_dir, config_path = {str(tmp_path)!r}, {str(tmp_path / "c.json")!r}
        reg = Registry(session_name="raw_hex_check", log_dir=log_dir, config_path=config_path)
        assert "raw_hex" in [name for name, _ in reg.system_ctx.factories.get_category_types("parser")]
        assert any("RawHexParser" in callback.__qualname__ for _priority, callback in _WARMUP_CALLBACKS)
        print("RAW_HEX_OK")
        """
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, cwd=str(REPO_ROOT))

    assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    assert "RAW_HEX_OK" in result.stdout
