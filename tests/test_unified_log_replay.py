import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry import IDRegistry
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.formatting import nb_estimate_batch_capacity, nb_format_log_row_batch
from blinkview.parsers.unified_log_replay import _LEVEL_BY_CHAR, _LINE_RE, _parse_ts_ns
from blinkview.utils.log_level import LogLevel


def make_bundle(timestamps, devices, levels, modules, messages):
    lengths = np.array([len(m.encode("utf-8")) for m in messages], dtype=dtypes.LEN_TYPE)
    offsets = np.zeros(len(messages), dtype=dtypes.OFFSET_TYPE)

    cursor = 0
    for i, m in enumerate(messages):
        offsets[i] = cursor
        cursor += len(m.encode("utf-8"))

    buffer = np.zeros(max(cursor, 1), dtype=dtypes.BYTE)
    cursor = 0
    for m in messages:
        b = m.encode("utf-8")
        if b:
            buffer[cursor : cursor + len(b)] = np.frombuffer(b, dtype=dtypes.BYTE)
        cursor += len(b)

    size = len(messages)
    return LogBundle(
        timestamps=np.array(timestamps, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.array(timestamps, dtype=dtypes.TS_TYPE),
        offsets=offsets,
        lengths=lengths,
        buffer=buffer,
        levels=np.array(levels, dtype=dtypes.LEVEL_TYPE),
        modules=np.array(modules, dtype=dtypes.ID_TYPE),
        devices=np.array(devices, dtype=dtypes.ID_TYPE),
        sequences=np.zeros(size, dtype=dtypes.SEQ_TYPE),
        pids=np.zeros(size, dtype=dtypes.ID_TYPE),
        tids=np.zeros(size, dtype=dtypes.ID_TYPE),
        ext_u32_1=np.zeros(size, dtype=dtypes.UINT32),
        ext_u32_2=np.zeros(size, dtype=dtypes.UINT32),
        ext_u64_1=np.zeros(size, dtype=dtypes.UINT64),
        size=np.array([size], dtype=np.int64),
        msg_cursor=np.array([cursor], dtype=np.int64),
        capacity=size,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
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
