from types import SimpleNamespace

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry import IDRegistry
from blinkview.ops.formatting import nb_estimate_batch_capacity, nb_format_log_row_batch
from blinkview.parsers.unified_log_replay import _LEVEL_BY_CHAR, _LINE_RE, UnifiedLogReplay, _parse_ts_ns
from blinkview.utils.log_level import LogLevel
from tests.fakes.log_bundle import make_log_bundle


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

    for i, line in enumerate(lines):
        m = _LINE_RE.match(line)
        assert m is not None, f"line did not match grammar: {line!r}"

        assert _parse_ts_ns(m.group("ts")) == timestamps[i]
        assert _LEVEL_BY_CHAR[m.group("level")] == levels[i]

        resolved_device = id_registry.get_device(m.group("device"))
        resolved_module = resolved_device.get_module(m.group("module"))
        assert resolved_device.id == devices[i]
        assert resolved_module.id == modules[i]
        assert m.group("message") == messages[i]


def make_line(ts_text: str, level_char: str, device: str, module: str, message: str) -> str:
    """Builds one line in the fixed grammar _LINE_RE expects (see module docstring)."""
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

    def warn(self, msg):
        self.warnings.append(msg)

    def exception(self, msg, exc=None):
        self.exceptions.append((msg, exc))

    def info(self, msg):
        self.infos.append(msg)


@pytest.fixture
def array_pool():
    return NumpyArrayPool(max_bytes=4 * 1024 * 1024)


def make_replay(log_parts, id_registry, array_pool, logger=None):
    replay = UnifiedLogReplay(log_parts)
    replay.shared = SimpleNamespace(id_registry=id_registry, array_pool=array_pool)
    replay.logger = logger
    return replay


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
        replay.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        rows = subscriber.batches[0]
        assert [r["message"] for r in rows] == ["hello world", "boom"]
        assert rows[0]["ts_ns"] == _parse_ts_ns("2026-01-01T00:00:00.000000")
        assert rows[1]["ts_ns"] == _parse_ts_ns("2026-01-01T00:00:01.500000")
        assert rows[0]["level"] == _LEVEL_BY_CHAR["I"]
        assert rows[1]["level"] == _LEVEL_BY_CHAR["E"]
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
        replay.subscribe(subscriber)

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
        replay.subscribe(subscriber)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["before", "after"]
        assert len(logger.warnings) == 1
        assert "unparseable line" in logger.warnings[0]

    def test_blank_lines_are_skipped(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text("\n" + make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "only") + "\n" + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.subscribe(subscriber)

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
        replay.subscribe(subscriber)

        replay.run()

        assert [len(b) for b in subscriber.batches] == [2, 2, 1]
        all_messages = [r["message"] for batch in subscriber.batches for r in batch]
        assert all_messages == [f"row{i}" for i in range(5)]

    def test_empty_file_distributes_nothing(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text("")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.subscribe(subscriber)

        replay.run()  # must not raise even though the batch is never populated

        assert subscriber.batches == []

    def test_stop_event_set_before_run_processes_nothing(self, tmp_path, id_registry, array_pool):
        part = tmp_path / "session.0000.log"
        part.write_text(make_line("2026-01-01T00:00:00.000000", "I", "dev", "log", "never seen") + "\n")

        subscriber = CapturingSubscriber()
        replay = make_replay([part], id_registry, array_pool)
        replay.subscribe(subscriber)
        replay._stop_event.set()

        replay.run()

        assert subscriber.batches == []

    def test_stop_event_set_mid_file_stops_processing_remaining_lines(self, tmp_path, id_registry, array_pool):
        """Covers the per-line stop check (distinct from the per-part one checked above): a stop
        requested while already partway through a file must not process the rest of that file's
        lines either."""

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
        replay.subscribe(subscriber)
        # Calls: 1) outer per-part check (False), 2) first line's check (False, "seen" is
        # processed), 3) second line's check (True, breaks before "never seen" is read).
        replay._stop_event = StopAfterNCalls(n=2)

        replay.run()

        assert len(subscriber.batches) == 1
        assert [r["message"] for r in subscriber.batches[0]] == ["seen"]

    def test_missing_log_part_is_caught_and_logged_without_raising(self, tmp_path, id_registry, array_pool):
        missing = tmp_path / "does_not_exist.log"

        subscriber = CapturingSubscriber()
        logger = FakeLogger()
        replay = make_replay([missing], id_registry, array_pool, logger=logger)
        replay.subscribe(subscriber)

        replay.run()  # FileNotFoundError from open() must be caught, not propagated

        assert subscriber.batches == []
        assert len(logger.exceptions) == 1


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

    for i, line in enumerate(lines):
        m = _LINE_RE.match(line)
        assert m is not None

        resolved_device = id_registry.get_device(m.group("device"))
        resolved_module = resolved_device.get_module(m.group("module"))

        # Must resolve to the *same* device/module id as the row that was
        # actually written for that line, not whichever device came first.
        assert resolved_device.id == devices[i]
        assert resolved_module.id == modules[i]
