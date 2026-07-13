from contextlib import contextmanager

import numpy as np
import pytest
from qtpy.QtCore import Qt

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import BYTE, ID_TYPE, LEN_TYPE, LEVEL_TYPE, OFFSET_TYPE, SEQ_TYPE, TS_TYPE, UINT32, UINT64
from blinkview.core.id_registry.registry import IDRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.module_snapshot import MAX_MSG_BYTES
from blinkview.core.types.log_batch import LogBundle
from blinkview.ui.widgets.log_table_viewer import LogTableCol, LogTableFilterProxy, LogTableModel
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel


def make_bundle(timestamps, rx_timestamps, devices, levels, modules, sequences, messages):
    """Builds a minimal LogBundle backing a fixed set of rows (mirrors tests/test_ops_segments.py)."""
    lengths = np.array([len(m) for m in messages], dtype=LEN_TYPE)
    offsets = np.zeros(len(messages), dtype=OFFSET_TYPE)

    cursor = 0
    for i, m in enumerate(messages):
        offsets[i] = cursor
        cursor += len(m.encode("utf-8"))

    buffer = np.zeros(max(cursor, 1), dtype=BYTE)
    cursor = 0
    for m in messages:
        b = m.encode("utf-8")
        if b:
            buffer[cursor : cursor + len(b)] = np.frombuffer(b, dtype=BYTE)
        cursor += len(b)

    size = len(messages)
    return LogBundle(
        timestamps=np.array(timestamps, dtype=TS_TYPE),
        rx_timestamps=np.array(rx_timestamps, dtype=TS_TYPE),
        offsets=offsets,
        lengths=lengths,
        buffer=buffer,
        levels=np.array(levels, dtype=LEVEL_TYPE),
        modules=np.array(modules, dtype=ID_TYPE),
        devices=np.array(devices, dtype=ID_TYPE),
        sequences=np.array(sequences, dtype=SEQ_TYPE),
        ext_u32_1=np.zeros(size, dtype=UINT32),
        ext_u32_2=np.zeros(size, dtype=UINT32),
        ext_u64_1=np.zeros(size, dtype=UINT64),
        size=np.array([size], dtype=np.int64),
        msg_cursor=np.array([cursor], dtype=np.int64),
        capacity=size,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


class FakeSegment:
    def __init__(self, bundle):
        self.bundle = bundle
        self.size = int(bundle.size[0])


class FakeIndicesHandle:
    def __init__(self, capacity=4096):
        self.array = np.zeros(capacity, dtype=np.int64)


class FakeLogPool:
    def __init__(self, latest_seq=0, segments=None):
        self._latest_seq = latest_seq
        self._segments = segments or []  # chronological order (oldest first)

    def latest_sequence(self):
        return self._latest_seq

    @contextmanager
    def get_reversed_snapshot(self):
        yield list(reversed(self._segments))

    @contextmanager
    def get_snapshot(self):
        yield list(self._segments)

    @contextmanager
    def acquire_indices_buffer(self):
        yield FakeIndicesHandle()


class FakeCentral:
    def __init__(self):
        self.log_pool = FakeLogPool()


class FakeSystemCtx:
    def __init__(self):
        self.array_pool = NumpyArrayPool()


class FakeRegistry:
    def __init__(self):
        self.central = FakeCentral()
        self.system_ctx = FakeSystemCtx()


class FakeGuiContext:
    """Minimal stand-in for GUIContext: only exposes what LogTableModel touches directly."""

    def __init__(self, id_registry):
        self.id_registry = id_registry
        self.registry = FakeRegistry()
        self.logger = PrintLogger("test")


class FakeFilterSidebar:
    def get_filter(self):
        return False, np.zeros(0, dtype=np.uint8)

    class action_show_non_essential:
        @staticmethod
        def isChecked():
            return True

    def sync_modules(self):
        pass


@pytest.fixture
def id_registry():
    return IDRegistry(NumpyArrayPool())


@pytest.fixture
def gui_context(id_registry):
    return FakeGuiContext(id_registry)


@pytest.fixture
def model(qapp, gui_context):
    log_filter = LogFilter(gui_context.id_registry, log_level=LogLevel.ALL.name_conf)
    m = LogTableModel(gui_context, log_filter, FakeFilterSidebar())
    m._valid_start = 0  # aligns row indices with array slots for the direct-write tests below
    return m


def _populate_row(model, row, ts, dev_id, level, mod_id, seq, message):
    model.ts[row] = ts
    model.rx_ts[row] = ts
    model.dev[row] = dev_id
    model.lvl[row] = level
    model.mod[row] = mod_id
    model.seq[row] = seq

    msg_bytes = message.encode("utf-8")
    off = row * MAX_MSG_BYTES
    model.msg_buffer[off : off + len(msg_bytes)] = np.frombuffer(msg_bytes, dtype=np.uint8)
    model.msg_offsets[row] = off
    model.msg_lengths[row] = len(msg_bytes)
    model._message_cache[row] = None


def _set_rows_and_reset(model, rows):
    """Populates rows via the test helper and properly notifies any attached proxy models."""
    model.beginResetModel()
    model.row_count = len(rows)
    for i, row in enumerate(rows):
        _populate_row(model, i, **row)
    model.endResetModel()


def test_rowcount_columncount_empty(model):
    assert model.rowCount() == 0
    assert model.columnCount() == len(LogTableCol)


def test_starts_in_live_mode(model):
    assert model.mode == "live"
    assert model.anchor_seq is None


def test_data_resolves_device_level_module_and_message(model, gui_context):
    device = gui_context.id_registry.get_device("esp32")
    module = device.get_module("wifi")

    model.row_count = 1
    _populate_row(
        model,
        0,
        ts=1_000_000_000,
        dev_id=device.id,
        level=LogLevel.WARN.value,
        mod_id=module.id,
        seq=1,
        message="hello",
    )

    assert model.data(model.index(0, LogTableCol.DEVICE)) == "esp32"
    assert model.data(model.index(0, LogTableCol.LEVEL)) == "WARNING"
    assert model.data(model.index(0, LogTableCol.MODULE)) == module.name
    assert model.data(model.index(0, LogTableCol.MESSAGE)) == "hello"
    assert model.data(model.index(0, LogTableCol.TIMESTAMP)) == model._format_ts(1_000_000_000)


def test_data_out_of_range_row_returns_none(model):
    model.row_count = 0
    assert model.data(model.index(0, LogTableCol.MESSAGE)) is None


def test_data_unknown_ids_fallback_to_question_mark(model):
    model.row_count = 1
    _populate_row(model, 0, ts=0, dev_id=999, level=0, mod_id=999, seq=1, message="x")
    assert model.data(model.index(0, LogTableCol.DEVICE)) == "?"
    assert model.data(model.index(0, LogTableCol.MODULE)) == "?"


def test_header_data(model):
    assert model.headerData(LogTableCol.TIMESTAMP, Qt.Horizontal) == "Time"
    assert model.headerData(LogTableCol.MESSAGE, Qt.Horizontal) == "Message"


def test_row_to_slot_uses_valid_start_offset(model):
    model._valid_start = 5
    assert model._row_to_slot(0) == 5
    assert model._row_to_slot(3) == 8


def test_seq_for_row_and_row_for_seq_round_trip(model):
    model.row_count = 3
    for i, seq in enumerate([10, 20, 30]):
        _populate_row(model, i, ts=i, dev_id=0, level=0, mod_id=0, seq=seq, message=f"m{i}")

    assert model.seq_for_row(1) == 20
    assert model.row_for_seq(20) == 1
    assert model.row_for_seq(999) == -1
    assert model.seq_for_row(-1) is None
    assert model.seq_for_row(99) is None


def test_clear_logs_resets_state_and_returns_to_live_mode(model, gui_context):
    model.row_count = 2
    _populate_row(model, 0, ts=0, dev_id=0, level=0, mod_id=0, seq=1, message="a")
    model.mode = "history"
    model.anchor_seq = 5
    gui_context.registry.central.log_pool._latest_seq = 42

    model.clear_logs()

    assert model.row_count == 0
    assert model._message_cache[0] is None
    assert model._last_backend_seq == 42
    assert model._valid_start == model.capacity
    assert model.mode == "live"
    assert model.anchor_seq is None


def test_set_viewport_rows_clamps_and_triggers_refetch_in_live_mode(model, gui_context):
    gui_context.registry.central.log_pool._latest_seq = 0

    # LIVE mode has no scrollbar, so the fetch bound must track the viewport exactly, even when
    # smaller than a previous, larger default - never clamp up past what actually fits on screen.
    model.set_viewport_rows(3)
    assert model.viewport_rows == 3

    model.set_viewport_rows(0)
    assert model.viewport_rows == 1

    model.set_viewport_rows(50)
    assert model.viewport_rows == 50


class TestLiveMode:
    def test_apply_updates_skips_when_backend_sequence_unchanged(self, model, gui_context):
        gui_context.registry.central.log_pool._latest_seq = 5
        model.apply_updates()
        assert model._last_backend_seq == 5

        # Poison row_count directly (bypassing the model) to detect whether a second call
        # actually rebuilds (it would reset row_count to 0 for an empty backend).
        model.row_count = 123
        model.prev_apply = 0  # bypass the 100ms throttle so we're testing the seq-unchanged skip
        model.apply_updates()

        assert model.row_count == 123  # untouched: backend sequence didn't change, so no rebuild ran

    def test_apply_updates_ignored_in_history_mode(self, model, gui_context):
        model.mode = "history"
        gui_context.registry.central.log_pool._latest_seq = 99
        model.row_count = 7

        model.apply_updates()

        assert model.row_count == 7  # apply_updates() is a live-mode-only mechanism

    def test_fetch_live_rebuilds_from_backend_segment(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["first", "second", "third"],
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])

        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 3
        assert model.data(model.index(0, LogTableCol.MESSAGE)) == "first"
        assert model.data(model.index(2, LogTableCol.MESSAGE)) == "third"

    def test_fetch_live_honors_kv_filter(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["status=ok id=1", "status=fail id=2", "status=ok id=3"],
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])

        model.log_filter.set_kv_filter("status=ok")
        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 2
        assert model.data(model.index(0, LogTableCol.MESSAGE)) == "status=ok id=1"
        assert model.data(model.index(1, LogTableCol.MESSAGE)) == "status=ok id=3"


class TestHistoryMode:
    def _make_pool(self, gui_context, device, module, count=20):
        messages = [f"m{i}" for i in range(count)]
        bundle = make_bundle(
            timestamps=list(range(count)),
            rx_timestamps=list(range(count)),
            devices=[device.id] * count,
            levels=[0] * count,
            modules=[module.id] * count,
            sequences=list(range(1, count + 1)),
            messages=messages,
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=count, segments=[FakeSegment(bundle)])
        return bundle

    def test_enter_history_mode_fetches_before_and_after_anchor(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")
        self._make_pool(gui_context, device, module, count=20)

        model.enter_history_mode(anchor_seq=10)

        assert model.mode == "history"
        assert model.row_count == 20  # small backend: entire history fits in the before+after window
        # Rows should be in ascending chronological order (message m0..m19 for seq 1..20).
        first_msg = model.data(model.index(0, LogTableCol.MESSAGE))
        last_msg = model.data(model.index(model.row_count - 1, LogTableCol.MESSAGE))
        assert first_msg == "m0"
        assert last_msg == "m19"

    def test_history_mode_anchor_at_first_message_has_no_before_rows(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")
        self._make_pool(gui_context, device, module, count=5)

        model.enter_history_mode(anchor_seq=1)

        assert model.mode == "history"
        # Nothing exists before sequence 1, so the window should start exactly at the anchor.
        assert model.seq_for_row(0) == 1

    def test_history_mode_honors_kv_filter_in_both_before_and_after_scans(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")

        messages = [f"status={'ok' if i % 2 == 0 else 'fail'} id={i}" for i in range(20)]
        bundle = make_bundle(
            timestamps=list(range(20)),
            rx_timestamps=list(range(20)),
            devices=[device.id] * 20,
            levels=[0] * 20,
            modules=[module.id] * 20,
            sequences=list(range(1, 21)),
            messages=messages,
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=20, segments=[FakeSegment(bundle)])

        model.log_filter.set_kv_filter("status=ok")
        model.enter_history_mode(anchor_seq=10)

        assert model.mode == "history"
        assert model.row_count == 10  # only the even-indexed (status=ok) rows survive
        for row in range(model.row_count):
            assert "status=ok" in model.data(model.index(row, LogTableCol.MESSAGE))

    def test_enter_live_mode_resets_anchor_and_refetches(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")
        self._make_pool(gui_context, device, module, count=5)

        model.enter_history_mode(anchor_seq=3)
        assert model.mode == "history"

        model.enter_live_mode()

        assert model.mode == "live"
        assert model.anchor_seq is None
        assert model.row_count == 5  # small backend: live view shows everything that exists


class TestLogTableFilterProxy:
    def test_empty_filter_accepts_everything(self, model):
        proxy = LogTableFilterProxy()
        proxy.setSourceModel(model)

        _set_rows_and_reset(model, [dict(ts=0, dev_id=0, level=0, mod_id=0, seq=1, message="anything")])

        assert proxy.rowCount() == 1

    def test_text_filter_matches_message(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")

        proxy = LogTableFilterProxy()
        proxy.setSourceModel(model)

        _set_rows_and_reset(
            model,
            [
                dict(ts=0, dev_id=device.id, level=0, mod_id=module.id, seq=1, message="connection lost"),
                dict(ts=1, dev_id=device.id, level=0, mod_id=module.id, seq=2, message="all good"),
            ],
        )

        proxy.set_filter_text("lost")

        assert proxy.rowCount() == 1
        assert proxy.data(proxy.index(0, LogTableCol.MESSAGE)) == "connection lost"

    def test_text_filter_matches_device_or_module_name(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        module = device.get_module("wifi")

        proxy = LogTableFilterProxy()
        proxy.setSourceModel(model)

        _set_rows_and_reset(model, [dict(ts=0, dev_id=device.id, level=0, mod_id=module.id, seq=1, message="hi")])

        proxy.set_filter_text("esp32")

        assert proxy.rowCount() == 1
