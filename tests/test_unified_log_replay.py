# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.id_registry import IDRegistry
from blinkview.ops.formatting import nb_estimate_batch_capacity, nb_format_log_row_batch
from blinkview.parsers.unified_log_replay import UnifiedLogReplay
from blinkview.utils.log_level import LogLevel
from tests.fakes.log_bundle import make_log_bundle


class FakeCentral(BaseDaemon):
    """Stand-in for CentralStorage: UnifiedLogReplay now pushes straight into
    `central.log_pool.batch_append()` + `central.distribute()` instead of going through
    subscribe()/distribute() on itself (see UnifiedLogReplay's class docstring) - real
    subscribers (CapturingSubscriber below) subscribe to this fake instead of to the replay
    reader directly."""

    def __init__(self):
        super().__init__()
        self.log_pool = SimpleNamespace(batch_append=lambda batch: None)


def make_bundle(timestamps, devices, levels, modules, messages):
    return make_log_bundle(
        timestamps,
        devices,
        levels,
        modules,
        [0] * len(messages),
        messages,
        has_pids=False,
        has_tids=False,
    )


def format_bundle_to_lines(bundle, id_registry: IDRegistry) -> list[str]:
    required = nb_estimate_batch_capacity(bundle, 120)
    out = np.zeros(required, dtype=dtypes.BYTE)
    sec_state = np.full(1, -1, dtype=np.int64)
    ts_cache = np.zeros(19, dtype=dtypes.BYTE)

    written = nb_format_log_row_batch(out, bundle, id_registry.bundle(), sec_state, ts_cache)
    text = out[:written].tobytes().decode("utf-8")
    return [line for line in text.split("\n") if line]


@pytest.fixture
def id_registry():
    return IDRegistry(NumpyArrayPool(max_bytes=4 * 1024 * 1024))


def test_round_trip_single_device(id_registry):
    device = id_registry.get_device("nrf52")
    mod_log = device.get_module("log")
    mod_sensor = device.get_module("sensor")

    timestamps = [1_718_527_173_123_456_000, 1_718_527_174_000_000_000]
    devices = [device.id, device.id]
    levels = [LogLevel.INFO.value, LogLevel.ERROR.value]
    modules = [mod_log.id, mod_sensor.id]
    messages = ["hello world", "temp=42.5C battery=87%"]

    bundle = make_bundle(timestamps, devices, levels, modules, messages)
    lines = format_bundle_to_lines(bundle, id_registry)
    assert len(lines) == 2

    subscriber = CapturingSubscriber()
    replay = make_replay_from_lines(lines, id_registry, NumpyArrayPool(max_bytes=4 * 1024 * 1024))
    replay.central.subscribe(subscriber)
    replay.run()

    assert len(subscriber.batches) == 1
    rows = subscriber.batches[0]
    for i, row in enumerate(rows):
        assert row["ts_ns"] == timestamps[i]
        assert row["level"] == levels[i]
        assert row["device"] == devices[i]
        assert row["module"] == modules[i]
        assert row["message"] == messages[i]


def make_line(ts_text: str, level_char: str, device: str, module: str, message: str) -> str:
    """Builds one line in the fixed grammar nb_scan_unified_log_lines expects (see
    ops/unified_log_scan.py and parsers/unified_log_replay.py's module docstring)."""
    return f"{ts_text}Z {level_char} {device} {module}: {message}"


class CapturingSubscriber:
    """Stand-in for CentralStorage - records each distributed batch's rows (decoded from the
    raw bundle, synchronously inside put()) before UnifiedLogReplay.run()'s `with batch:`
    releases it back to the pool."""

    def __init__(self):
        self.batches: list[list[dict]] = []

    def put(self, batch):
        b = batch.bundle
        rows = []
        for i in range(batch.size):
            start = int(b.offsets[i])
            length = int(b.lengths[i])
            rows.append(
                {
                    "ts_ns": int(b.timestamps[i]),
                    "level": int(b.levels[i]),
                    "module": int(b.modules[i]),
                    "device": int(b.devices[i]),
                    "message": bytes(b.buffer[start : start + length]).decode("utf-8"),
                }
            )
        self.batches.append(rows)


class FakeLogger:
    def __init__(self):
        self.warnings = []
        self.exceptions = []
        self.infos = []

    def warn(self, msg, *args):
        self.warnings.append(msg % args if args else msg)

    def exception(self, msg, *args, exc=None):
        self.exceptions.append((msg % args if args else msg, exc))

    def info(self, msg, *args):
        self.infos.append(msg % args if args else msg)


@pytest.fixture
def array_pool():
    return NumpyArrayPool(max_bytes=4 * 1024 * 1024)


def make_replay(log_parts, id_registry, array_pool, logger=None):
    replay = UnifiedLogReplay(log_parts, central=FakeCentral())
    replay.shared = SimpleNamespace(id_registry=id_registry, array_pool=array_pool)
    replay.logger = logger
    return replay


def make_replay_from_lines(lines, id_registry, array_pool, logger=None, tmp_path=None):
    import tempfile
    from pathlib import Path

    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    part = tmp_path / "session.0000.log"
    part.write_text("\n".join(lines) + "\n" if lines else "")
    return make_replay([part], id_registry, array_pool, logger=logger)


class TestRun:
    """Exercises UnifiedLogReplay.run() directly (synchronously, no thread) - it's a one-shot
    read-then-distribute loop, so calling it in-process is equivalent to what start()/_run_wrapper
    would do and avoids flaky thread-timing in tests."""

    def test_parses_lines_and_distributes_a_single_batch(self, tmp_path, id_registry, array_pool):
        device = id_registry.get_device("nrf52")
        module = device.get_module("log")

        part = tmp_path / "session.0000.log"
        part.write_text(
            make_line("2026-01-01T00:00:00.000000", "I", "nrf52", "log", "hello world")
            + "\n"
            + make_line("2026-01-01T00:00:01.500000", "E", "nrf52", "log", "boom")
            + "\n"
        )

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        rows = subscriber.batches[0]
        assert [r["message"] for r in rows] == ["hello world", "boom"]
        assert rows[0]["ts_ns"] == 1_767_225_600_000_000_000
        assert rows[1]["ts_ns"] == 1_767_225_601_500_000_000
        assert rows[0]["level"] == LogLevel.INFO.value
        assert rows[1]["level"] == LogLevel.ERROR.value
        assert rows[0]["device"] == rows[1]["device"] == device.id
        assert rows[0]["module"] == rows[1]["module"] == module.id

    def test_reads_multiple_log_parts_in_order(self, tmp_path, id_registry, array_pool):
        device = id_registry.get_device("dev")
        device.get_module("log")

        part1 = tmp_path / "session.0000.log"
        part1.write_text(make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "first") + "\n")
        part2 = tmp_path / "session.0001.log"
        part2.write_text(make_line("2026-01-01T00:00:01.000000", "I", "dev", "log", "second") + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay([part1, part2], id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["first", "second"]

    def test_reads_a_compressed_part_identically_to_an_uncompressed_one(self, tmp_path, id_registry, array_pool):
        """A .log.zst part (rotated-away or cleanly-closed - see storage/log_file_archive.py)
        must decode identically to the same content read uncompressed."""
        from blinkview.storage.log_file_archive import compress_log_part_file

        device = id_registry.get_device("dev")
        device.get_module("log")

        raw_part = tmp_path / "session.0000.log"
        raw_part.write_text(
            make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "hello world")
            + "\n"
            + make_line("2026-01-01T00:00:01.500000", "E", "dev", "log", "boom")
            + "\n"
        )
        compressed_part = compress_log_part_file(raw_part)
        raw_part.unlink()  # only the .log.zst sibling remains, matching production after rotation

        subscriber = CapturingSubscriber()
        replay = make_replay([compressed_part], id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["hello world", "boom"]

    def test_reads_a_mixed_session_of_compressed_and_uncompressed_parts_in_order(
        self, tmp_path, id_registry, array_pool
    ):
        """A session where earlier parts got rotated (and compressed) but the process was killed
        before the final part's shutdown-time compression ran - must still parse everything, in
        the right order, regardless of which parts are compressed."""
        from blinkview.storage.log_file_archive import compress_log_part_file

        device = id_registry.get_device("dev")
        device.get_module("log")

        part0 = tmp_path / "session.0000.log"
        part0.write_text(make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "first") + "\n")
        compressed_part0 = compress_log_part_file(part0)
        part0.unlink()

        part1 = tmp_path / "session.0001.log"  # never compressed - simulates a crash mid-session
        part1.write_text(make_line("2026-01-01T00:00:01.000000", "I", "dev", "log", "second") + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay(sorted([compressed_part0, part1]), id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["first", "second"]

    def test_skips_unparseable_lines_and_continues(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text(
            make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "before")
            + "\n"
            + "this line does not match the grammar at all\n"
            + make_line("2026-01-01T00:00:01.000000", "I", "dev", "log", "after")
            + "\n"
        )

        subscriber = CapturingSubscriber()
        logger = FakeLogger()
        replay = make_replay([part], id_registry, array_pool, logger=logger)
        replay.central.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["before", "after"]
        assert len(logger.warnings) == 1
        assert "unparseable line" in logger.warnings[0]

    def test_many_malformed_lines_are_capped_with_a_rollup_warning(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        bad_lines = "\n".join(f"not valid {i}" for i in range(UnifiedLogReplay.MAX_MALFORMED + 5))
        part.write_text(bad_lines + "\n")

        subscriber = CapturingSubscriber()
        logger = FakeLogger()
        replay = make_replay([part], id_registry, array_pool, logger=logger)
        replay.central.subscribe(subscriber)

        replay.run()

        assert subscriber.batches == []
        # One warning per capped malformed line, plus one rollup warning for the overflow.
        assert len(logger.warnings) == UnifiedLogReplay.MAX_MALFORMED + 1
        assert "further unparseable" in logger.warnings[-1]

    def test_blank_lines_are_skipped(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text("\n" + make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "only") + "\n" + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["only"]

    def test_splits_into_multiple_batches_when_capacity_exceeded(self, tmp_path, id_registry):
        # NumpyArrayPool.acquire always rounds a request up to at least min_bytes worth of
        # elements (see array_pool.py's _calc_slab_size) - the default min_bytes=1024 would
        # silently give a "2-row" request a true capacity of 128 int64 rows, defeating the point
        # of this test. A tiny min_bytes here makes the granularity match MAX_BATCH_ROWS exactly.
        tiny_pool = NumpyArrayPool(min_bytes=1, max_bytes=4 * 1024 * 1024)
        lines = [make_line(f"2026-01-01T00:00:0{i}.000000", "I", "dev", "log", f"row{i}") for i in range(5)]
        part = tmp_path / "session.0000.log"
        part.write_text("\n".join(lines) + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, tiny_pool)
        replay.MAX_BATCH_ROWS = 2  # force a split well before the real 4096-row default
        replay.central.subscribe(subscriber)

        replay.run()

        assert [len(b) for b in subscriber.batches] == [2, 2, 1]
        all_messages = [r["message"] for batch in subscriber.batches for r in batch]
        assert all_messages == [f"row{i}" for i in range(5)]

    def test_empty_file_distributes_nothing(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text("")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()  # must not raise even though the file is empty (mmap can't map 0 bytes)

        assert subscriber.batches == []

    def test_stop_event_set_before_run_processes_nothing(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text(make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "never seen") + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.central.subscribe(subscriber)
        replay._stop_event.set()

        replay.run()

        assert subscriber.batches == []

    def test_stop_event_set_mid_file_stops_processing_remaining_scan_batches(self, tmp_path, id_registry, array_pool):
        """Stop-checking now happens once per scan-batch (up to MAX_BATCH_ROWS lines at a time,
        parsed inside a single Numba call) rather than per individual line - checking per line
        would mean re-entering Python between every row, defeating the point of batching the
        parse loop into Numba. Forcing MAX_BATCH_ROWS=1 here makes each scan-batch cover exactly
        one line, so stopping after the first batch is still observable."""

        class StopAfterNCalls:
            def __init__(self, n):
                self._calls = 0
                self._n = n

            def is_set(self):
                self._calls += 1
                return self._calls > self._n

        part = tmp_path / "session.0000.log"
        part.write_text(
            make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "seen")
            + "\n"
            + make_line("2026-01-01T00:00:01.000000", "I", "dev", "log", "never seen")
            + "\n"
        )

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.MAX_BATCH_ROWS = 1
        replay.central.subscribe(subscriber)
        # Calls: 1) outer per-part check (False), 2) first scan-batch's check (False, "seen" is
        # scanned and pushed), 3) second scan-batch's check (True, stops before "never seen").
        replay._stop_event = StopAfterNCalls(n=2)

        replay.run()

        all_messages = [r["message"] for batch in subscriber.batches for r in batch]
        assert all_messages == ["seen"]

    def test_missing_log_part_is_caught_and_logged_without_raising(self, tmp_path, id_registry, array_pool):
        missing = tmp_path / "does_not_exist.log"

        subscriber = CapturingSubscriber()
        logger = FakeLogger()
        replay = make_replay([missing], id_registry, array_pool, logger=logger)
        replay.central.subscribe(subscriber)

        replay.run()  # FileNotFoundError from os.path.getsize() must be caught, not propagated

        assert subscriber.batches == []
        assert len(logger.exceptions) == 1

    def test_pushes_batches_straight_into_central_log_pool(self, tmp_path, id_registry, array_pool):
        """Regression test: this reader no longer feeds central via subscribe()/distribute() on
        itself - it must call central.log_pool.batch_append() directly (see UnifiedLogReplay._push
        and the class docstring), so central's own sequence numbers actually advance."""
        part = tmp_path / "session.0000.log"
        part.write_text(
            make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "first")
            + "\n"
            + make_line("2026-01-01T00:00:01.000000", "I", "dev", "log", "second")
            + "\n"
        )

        replay = make_replay([part], id_registry, array_pool)
        appended = []
        replay.central.log_pool = SimpleNamespace(batch_append=lambda batch: appended.append(batch.size))

        replay.run()

        assert appended == [2]

    def test_on_part_progress_is_called_once_per_part_with_1_based_index_and_total(
        self, tmp_path, id_registry, array_pool
    ):
        part1 = tmp_path / "session.0000.log"
        part1.write_text(make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "first") + "\n")
        part2 = tmp_path / "session.0001.log"
        part2.write_text(make_line("2026-01-01T00:00:01.000000", "I", "dev", "log", "second") + "\n")

        calls = []
        replay = UnifiedLogReplay(
            [part1, part2],
            central=FakeCentral(),
            on_part_progress=lambda i, total, label: calls.append((i, total, label)),
        )
        replay.shared = SimpleNamespace(id_registry=id_registry, array_pool=array_pool)

        replay.run()

        assert calls == [(1, 2, part1.name), (2, 2, part2.name)]

    def test_on_finished_is_called_exactly_once_after_all_parts_processed(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text(make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "only") + "\n")

        calls = []
        replay = UnifiedLogReplay([part], central=FakeCentral(), on_finished=lambda: calls.append(True))
        replay.shared = SimpleNamespace(id_registry=id_registry, array_pool=array_pool)

        replay.run()

        assert calls == [True]

    def test_on_finished_is_called_even_when_a_log_part_is_missing(self, tmp_path, id_registry, array_pool):
        """on_finished lives in the run() method's `finally` block, so a caller relying on it to
        e.g. resume paused ingest (see main_window.start_replay) isn't left hanging just because
        one part raised."""
        missing = tmp_path / "does_not_exist.log"

        calls = []
        logger = FakeLogger()
        replay = UnifiedLogReplay([missing], central=FakeCentral(), on_finished=lambda: calls.append(True))
        replay.shared = SimpleNamespace(id_registry=id_registry, array_pool=array_pool)
        replay.logger = logger

        replay.run()

        assert calls == [True]

    def test_same_module_name_under_different_devices_resolves_to_different_ids(
        self, tmp_path, id_registry, array_pool
    ):
        """Regression test for the Numba-backed device/module resolution (ops/id_resolution.py):
        module names are only unique *within* a device, so two devices both logging through a
        module literally named "log" must not collapse into one shared id, whether resolved via
        the temp-tracker (first occurrence) or the permanent scoped table (repeats)."""
        part = tmp_path / "session.0000.log"
        part.write_text(
            make_line("2026-01-01T00:00:00.000000", "I", "client", "log", "hello from client")
            + "\n"
            + make_line("2026-01-01T00:00:01.000000", "I", "server", "log", "hello from server")
            + "\n"
            + make_line("2026-01-01T00:00:02.000000", "I", "client", "log", "second from client")
            + "\n"
            + make_line("2026-01-01T00:00:03.000000", "I", "server", "log", "second from server")
            + "\n"
        )

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.central.subscribe(subscriber)

        replay.run()

        rows = {r["message"]: r for batch in subscriber.batches for r in batch}
        client_log_id = rows["hello from client"]["module"]
        server_log_id = rows["hello from server"]["module"]
        assert client_log_id != server_log_id
        assert rows["second from client"]["module"] == client_log_id
        assert rows["second from server"]["module"] == server_log_id


def test_round_trip_interleaved_devices(id_registry):
    """Confirms per-row device/module resolution works for a log with multiple
    interleaved devices - the exact case BinaryParser (bound to one device per
    instance) cannot handle."""
    dev_a = id_registry.get_device("device_a")
    dev_b = id_registry.get_device("device_b")
    mod_a = dev_a.get_module("app.main")
    mod_b = dev_b.get_module("radio")

    timestamps = [1_718_527_173_000_000_000, 1_718_527_173_500_000_000, 1_718_527_174_000_000_000]
    devices = [dev_a.id, dev_b.id, dev_a.id]
    levels = [LogLevel.DEBUG.value, LogLevel.WARN.value, LogLevel.INFO.value]
    modules = [mod_a.id, mod_b.id, mod_a.id]
    messages = ["boot complete", "rssi low", "tick"]

    bundle = make_bundle(timestamps, devices, levels, modules, messages)
    lines = format_bundle_to_lines(bundle, id_registry)
    assert len(lines) == 3

    subscriber = CapturingSubscriber()
    replay = make_replay_from_lines(lines, id_registry, NumpyArrayPool(max_bytes=4 * 1024 * 1024))
    replay.central.subscribe(subscriber)
    replay.run()

    assert len(subscriber.batches) == 1
    rows = subscriber.batches[0]
    for i, row in enumerate(rows):
        # Must resolve to the *same* device/module id as the row that was actually written for
        # that line, not whichever device came first.
        assert row["device"] == devices[i]
        assert row["module"] == modules[i]
