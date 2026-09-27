# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Per-tab Clear acts as a hard lower bound (LogSegmentScanner.floor_seq) for every fetch -
live tail, filter-change refetch and history paging - and right-clicking Clear lifts it again.
Scanner-level tests plus real-widget coverage for both LogViewerWidget and LogTableViewerWidget,
since the bug this guards against (history paging / filter refetch resurfacing pre-Clear rows)
lived in the widgets' fetch wiring, not in any single kernel."""

import pytest

from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.log_table_viewer import LogTableViewerWidget
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.utils.log_level import LogLevel
from tests.fakes.devices import esp32_wifi
from tests.fakes.log_bundle import make_log_bundle as make_bundle
from tests.fakes.log_pool import FakeLogPool, FakeSegment
from tests.fakes.real_registry import make_real_gui_context, make_real_registry
from tests.test_log_fetch import RecordingConsumer, make_scanner

OLD_COUNT = 20
NEW_COUNT = 5


# --- Scanner level ---------------------------------------------------------------------------


def _scanner_with_rows(id_registry, log_filter, count=20, segment_size=5):
    """`count` rows (seq 1..count, ts == seq) split across several segments, so the floor's
    per-segment skip/break paths get exercised and not just the in-kernel bound."""
    device, module = esp32_wifi(id_registry)
    segments = []
    for start in range(1, count + 1, segment_size):
        seqs = list(range(start, min(start + segment_size, count + 1)))
        segments.append(
            FakeSegment(
                make_bundle(
                    timestamps=seqs,
                    devices=[device.id] * len(seqs),
                    levels=[LogLevel.INFO.value] * len(seqs),
                    modules=[module.id] * len(seqs),
                    sequences=seqs,
                    messages=[f"m{s}" for s in seqs],
                )
            )
        )
    pool = FakeLogPool(latest_seq=count, segments=segments)
    return make_scanner(id_registry, pool, log_filter)


class TestScannerFloor:
    def test_full_rescan_stops_at_floor(self, id_registry, log_filter):
        scanner = _scanner_with_rows(id_registry, log_filter)
        scanner.floor_seq = 12

        consumer = RecordingConsumer()
        result = scanner.scan_tail(start_seq=SEQ_NONE, max_rows=100, consume=consumer)

        assert sorted(consumer.all_seqs) == list(range(13, 21))
        assert result.reached_live_edge is True

    def test_watermark_above_floor_still_wins(self, id_registry, log_filter):
        scanner = _scanner_with_rows(id_registry, log_filter)
        scanner.floor_seq = 5

        consumer = RecordingConsumer()
        scanner.scan_tail(start_seq=15, max_rows=100, consume=consumer)

        assert sorted(consumer.all_seqs) == list(range(16, 21))

    def test_history_seq_anchor_before_is_bounded_and_reaches_start(self, id_registry, log_filter):
        scanner = _scanner_with_rows(id_registry, log_filter)
        scanner.floor_seq = 12

        before, after = RecordingConsumer(), RecordingConsumer()
        result = scanner.scan_history_window(
            anchor_seq=16, before_cap=100, after_cap=100, consume_before=before, consume_after=after
        )

        assert sorted(before.all_seqs) == [13, 14, 15]
        assert sorted(after.all_seqs) == list(range(16, 21))
        assert result.oldest_seq == 13
        assert result.reached_start is True

    def test_history_anchor_below_floor_shows_only_rows_past_floor(self, id_registry, log_filter):
        scanner = _scanner_with_rows(id_registry, log_filter)
        scanner.floor_seq = 12

        before, after = RecordingConsumer(), RecordingConsumer()
        result = scanner.scan_history_window(
            anchor_seq=4, before_cap=100, after_cap=100, consume_before=before, consume_after=after
        )

        assert before.all_seqs == []
        assert sorted(after.all_seqs) == list(range(13, 21))
        assert result.newest_seq == 20

    def test_history_ts_anchor_respects_floor(self, id_registry, log_filter):
        scanner = _scanner_with_rows(id_registry, log_filter)
        scanner.floor_seq = 12

        before, after = RecordingConsumer(), RecordingConsumer()
        scanner.scan_history_window(
            anchor_ts=8, before_cap=100, after_cap=100, consume_before=before, consume_after=after
        )

        assert before.all_seqs == []
        assert sorted(after.all_seqs) == list(range(13, 21))

    def test_lifting_floor_restores_everything(self, id_registry, log_filter):
        scanner = _scanner_with_rows(id_registry, log_filter)
        scanner.floor_seq = 12
        scanner.floor_seq = SEQ_NONE

        before, after = RecordingConsumer(), RecordingConsumer()
        scanner.scan_history_window(
            anchor_seq=16, before_cap=100, after_cap=100, consume_before=before, consume_after=after
        )

        assert sorted(before.all_seqs) == list(range(1, 16))


# --- Real widgets ----------------------------------------------------------------------------


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "log_view_clear_floor_test")
    yield reg
    reg.stop()


def _push(registry, device, module, count, text):
    array_pool = registry.system_ctx.array_pool
    base = registry.now_ns()
    src = array_pool.create(PooledLogBatch, count, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(count):
            ts = base + i * 1_000_000
            src.insert_any(ts, ts, f"{text}{i}".encode("ascii"), level=0, module=module.id, device=device.id)
        registry.central.log_pool.batch_append(src)


def _setup(qtbot, registry, widget_cls):
    """OLD_COUNT rows pushed, widget opened and caught up, Clear clicked, NEW_COUNT rows pushed."""
    device = registry.id_registry.get_device("clearfloor")
    module = device.get_module("mod1")
    _push(registry, device, module, OLD_COUNT, "oldrow")
    registry.playback_clock.tick(registry.now_ns())

    gui_context = make_real_gui_context(registry)
    w = widget_cls(gui_context)
    qtbot.addWidget(w)
    w.resize(800, 600)
    _tick(w)

    w.action_clear.trigger()
    _push(registry, device, module, NEW_COUNT, "newrow")
    _tick(w)
    return w


def _tick(w):
    w.prev_apply = 0
    if hasattr(w, "model"):
        w.model.prev_apply = 0  # LogTableStore has its own LIVE-fetch throttle
    w.apply_updates()


def _right_click_clear(w):
    button = w.toolbar.widgetForAction(w.action_clear)
    button.customContextMenuRequested.emit(button.rect().center())


def _text(w):
    return w.text_area.document().toPlainText()


def _table_seqs(w):
    return [w.model.seq_for_row(r) for r in range(w.model.row_count)]


class TestLogViewerWidgetFloor:
    def test_live_tail_after_clear_shows_only_new(self, qtbot, registry):
        w = _setup(qtbot, registry, LogViewerWidget)
        assert "newrow" in _text(w)
        assert "oldrow" not in _text(w)

    def test_history_paging_does_not_cross_floor(self, qtbot, registry):
        w = _setup(qtbot, registry, LogViewerWidget)
        w._reanchor_history(w._live_seqs[-1])
        assert w.view_mode == LogViewMode.HISTORY
        assert w.history_reached_start is True
        assert "oldrow" not in _text(w)

        w._reanchor_history(OLD_COUNT // 2)  # an anchor from before the Clear
        assert "oldrow" not in _text(w)

    def test_filter_refresh_does_not_resurface_old_rows(self, qtbot, registry):
        w = _setup(qtbot, registry, LogViewerWidget)
        w._apply_kv_filter_text("")
        w._refresh_view()
        _tick(w)
        assert "oldrow" not in _text(w)
        assert "newrow" in _text(w)

    def test_right_click_clear_restores_old_rows(self, qtbot, registry):
        w = _setup(qtbot, registry, LogViewerWidget)
        assert "right-click" in w.action_clear.toolTip()

        _right_click_clear(w)
        _tick(w)

        assert w._scanner.floor_seq == SEQ_NONE
        assert "oldrow" in _text(w)
        assert "newrow" in _text(w)
        assert "right-click" not in w.action_clear.toolTip()


class TestLogTableViewerWidgetFloor:
    def test_live_tail_after_clear_shows_only_new(self, qtbot, registry):
        w = _setup(qtbot, registry, LogTableViewerWidget)
        seqs = _table_seqs(w)
        assert len(seqs) == NEW_COUNT
        assert min(seqs) > OLD_COUNT

    def test_filter_change_full_refetch_respects_floor(self, qtbot, registry):
        w = _setup(qtbot, registry, LogTableViewerWidget)
        w.model.reload_and_redraw()
        seqs = _table_seqs(w)
        assert seqs and min(seqs) > OLD_COUNT

    def test_history_paging_does_not_cross_floor(self, qtbot, registry):
        w = _setup(qtbot, registry, LogTableViewerWidget)
        w._reanchor_history(OLD_COUNT + NEW_COUNT)
        assert w.model.mode == LogViewMode.HISTORY
        seqs = _table_seqs(w)
        assert seqs and min(seqs) > OLD_COUNT

    def test_right_click_clear_restores_old_rows(self, qtbot, registry):
        w = _setup(qtbot, registry, LogTableViewerWidget)
        _right_click_clear(w)
        _tick(w)

        assert w.model.floor_seq == SEQ_NONE
        seqs = _table_seqs(w)
        assert min(seqs) <= OLD_COUNT
        assert max(seqs) == OLD_COUNT + NEW_COUNT
