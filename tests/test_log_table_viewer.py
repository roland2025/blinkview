# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from unittest.mock import MagicMock

import numpy as np
import pytest

from blinkview.core.module_snapshot import MAX_MSG_BYTES
from blinkview.ui.widgets.log_table_viewer import LogTableCanvas, LogTableCol, LogTableStore, _COLUMN_LABELS
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from tests.fakes.devices import esp32_wifi
from tests.fakes.log_bundle import make_log_bundle as _make_log_bundle
from tests.fakes.log_pool import FakeIndicesHandle, FakeLogPool, FakeSegment
from tests.fakes.registry import FakeCentral, FakeGuiContext, FakeRegistry, FakeSystemCtx


def make_bundle(timestamps, rx_timestamps, devices, levels, modules, sequences, messages, pids=None, tids=None):
    """Builds a minimal LogBundle backing a fixed set of rows."""
    return _make_log_bundle(
        timestamps,
        devices,
        levels,
        modules,
        sequences,
        messages,
        rx_timestamps=rx_timestamps,
        pids=pids,
        tids=tids,
    )


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
def gui_context(id_registry):
    return FakeGuiContext(id_registry)


@pytest.fixture
def model(qapp, gui_context):
    log_filter = LogFilter(gui_context.id_registry, log_level=LogLevel.ALL.name_conf)
    m = LogTableStore(gui_context, log_filter, FakeFilterSidebar())
    m._valid_start = 0  # aligns row indices with array slots for the direct-write tests below
    return m


def _populate_row(model, row, ts, dev_id, level, mod_id, seq, message, pid=0, tid=0):
    model.ts[row] = ts
    model.rx_ts[row] = ts
    model.dev[row] = dev_id
    model.lvl[row] = level
    model.mod[row] = mod_id
    model.seq[row] = seq
    model.pid[row] = pid
    model.tid[row] = tid

    msg_bytes = message.encode("utf-8")
    off = row * MAX_MSG_BYTES
    model.msg_buffer[off : off + len(msg_bytes)] = np.frombuffer(msg_bytes, dtype=np.uint8)
    model.msg_offsets[row] = off
    model.msg_lengths[row] = len(msg_bytes)
    model._message_cache[row] = None


def test_rowcount_columncount_empty(model):
    assert model.row_count == 0
    assert len(LogTableCol) == 8


def test_starts_in_live_mode(model):
    assert model.mode == LogViewMode.LIVE
    assert model.anchor_seq is None


def test_data_resolves_device_level_module_and_message(model, gui_context):
    device, module = esp32_wifi(gui_context.id_registry)

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

    assert model.get_cell(0, LogTableCol.DEVICE) == "esp32"
    assert model.get_cell(0, LogTableCol.LEVEL) == "WARNING"
    assert model.get_cell(0, LogTableCol.MODULE) == module.name
    assert model.get_cell(0, LogTableCol.MESSAGE) == "hello"


def test_data_process_and_thread_fall_back_to_dash_when_unset(model, gui_context):
    device, module = esp32_wifi(gui_context.id_registry)

    model.row_count = 1
    _populate_row(model, 0, ts=0, dev_id=device.id, level=0, mod_id=module.id, seq=1, message="hi")

    assert model.get_cell(0, LogTableCol.PROCESS) == "-"
    assert model.get_cell(0, LogTableCol.THREAD) == "-"


def test_data_thread_shows_raw_tid(model, gui_context):
    device, module = esp32_wifi(gui_context.id_registry)

    model.row_count = 1
    _populate_row(model, 0, ts=0, dev_id=device.id, level=0, mod_id=module.id, seq=1, message="hi", pid=123, tid=456)

    assert model.get_cell(0, LogTableCol.THREAD) == "456"


def test_data_process_resolves_via_shared_pid_history(model, gui_context):
    device, module = esp32_wifi(gui_context.id_registry)

    key = (device.id << 32) | 123
    gui_context.registry.pid_history.update(key, "com.example.app", 1_000)

    model.row_count = 1
    _populate_row(
        model, 0, ts=1_500, dev_id=device.id, level=0, mod_id=module.id, seq=1, message="hi", pid=123, tid=456
    )

    assert model.get_cell(0, LogTableCol.PROCESS) == "com.example.app"


def test_data_process_falls_back_to_dash_when_pid_unresolved(model, gui_context):
    device, module = esp32_wifi(gui_context.id_registry)

    model.row_count = 1
    # pid is set, but nothing was ever recorded in pid_history for it.
    _populate_row(model, 0, ts=0, dev_id=device.id, level=0, mod_id=module.id, seq=1, message="hi", pid=999)

    assert model.get_cell(0, LogTableCol.PROCESS) == "-"


def test_data_out_of_range_row_returns_none(model):
    model.row_count = 0
    assert model.get_cell(0, LogTableCol.MESSAGE) is None


def test_data_unknown_ids_fallback_to_question_mark(model):
    model.row_count = 1
    _populate_row(model, 0, ts=0, dev_id=999, level=0, mod_id=999, seq=1, message="x")
    assert model.get_cell(0, LogTableCol.DEVICE) == "?"
    assert model.get_cell(0, LogTableCol.MODULE) == "?"


def test_header_data():
    assert _COLUMN_LABELS[LogTableCol.TIMESTAMP] == "Time"
    assert _COLUMN_LABELS[LogTableCol.MESSAGE] == "Message"


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
    model.mode = LogViewMode.HISTORY
    model.anchor_seq = 5
    gui_context.registry.central.log_pool._latest_seq = 42

    model.clear_logs()

    assert model.row_count == 0
    assert model._message_cache[0] is None
    assert model._last_backend_seq == 42
    assert model._valid_start == model.capacity
    assert model.mode == LogViewMode.LIVE
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
        model.mode = LogViewMode.HISTORY
        gui_context.registry.central.log_pool._latest_seq = 99
        model.row_count = 7

        model.apply_updates()

        assert model.row_count == 7  # apply_updates() is a live-mode-only mechanism

    def test_fetch_live_rebuilds_from_backend_segment(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)

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
        assert model.get_cell(0, LogTableCol.MESSAGE) == "first"
        assert model.get_cell(2, LogTableCol.MESSAGE) == "third"

    def test_fetch_live_honors_kv_filter(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)

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
        assert model.get_cell(0, LogTableCol.MESSAGE) == "status=ok id=1"
        assert model.get_cell(1, LogTableCol.MESSAGE) == "status=ok id=3"

    def test_fetch_live_honors_backend_text_filter_on_message(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["connection lost", "all good", "connection restored"],
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])

        model.log_filter.set_text_filter("connection")
        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 2
        assert model.get_cell(0, LogTableCol.MESSAGE) == "connection lost"
        assert model.get_cell(1, LogTableCol.MESSAGE) == "connection restored"

    def test_fetch_live_backend_text_filter_matches_device_name(self, model, gui_context):
        device = gui_context.id_registry.get_device("esp32")
        other_device = gui_context.id_registry.get_device("nrf52")
        module = device.get_module("wifi")

        bundle = make_bundle(
            timestamps=[1, 2],
            rx_timestamps=[1, 2],
            devices=[device.id, other_device.id],
            levels=[0, 0],
            modules=[module.id, module.id],
            sequences=[1, 2],
            messages=["hi", "hi"],
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=2, segments=[FakeSegment(bundle)])

        model.log_filter.set_text_filter("esp32")
        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 1
        assert model.get_cell(0, LogTableCol.DEVICE) == "esp32"

    def test_fetch_live_backend_text_filter_re_scans_beyond_the_small_fetched_window(self, model, gui_context):
        """The old Qt-proxy approach only filtered whatever rows had already been fetched into
        the live window - the backend filter must re-scan the whole segment so a match that's
        NOT among the most recent rows still surfaces and fills the viewport."""
        device, module = esp32_wifi(gui_context.id_registry)

        count = 50
        messages = ["needle" if i == 0 else f"noise {i}" for i in range(count)]
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

        model.log_filter.set_text_filter("needle")
        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 1
        assert model.get_cell(0, LogTableCol.MESSAGE) == "needle"

    def test_fetch_live_incremental_carries_forward_previous_matches(self, model, gui_context):
        """An append-only tick (no filter change) should be served by the incremental path:
        previously-matched rows are carried forward rather than re-derived, and the newly
        arrived matching rows are appended after them in order."""
        device, module = esp32_wifi(gui_context.id_registry)

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["first", "second", "third"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        gui_context.registry.central.log_pool = pool

        model.prev_apply = 0
        model.apply_updates()
        assert model.row_count == 3

        # Simulate real segment growth: the same (single) segment gains more rows, matching how
        # CircularLogPool grows an active segment in place rather than rotating for every insert.
        bundle2 = make_bundle(
            timestamps=[1, 2, 3, 4, 5],
            rx_timestamps=[1, 2, 3, 4, 5],
            devices=[device.id] * 5,
            levels=[0] * 5,
            modules=[module.id] * 5,
            sequences=[1, 2, 3, 4, 5],
            messages=["first", "second", "third", "fourth", "fifth"],
        )
        pool._segments = [FakeSegment(bundle2)]
        pool._latest_seq = 5

        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 5
        assert model.get_cell(0, LogTableCol.MESSAGE) == "first"
        assert model.get_cell(3, LogTableCol.MESSAGE) == "fourth"
        assert model.get_cell(4, LogTableCol.MESSAGE) == "fifth"

    def test_fetch_live_incremental_evicts_oldest_rows_beyond_viewport(self, model, gui_context):
        """When new matches overflow the viewport, the oldest carried-over rows must be evicted
        (not arbitrary ones), and no sequence number should ever appear twice."""
        device, module = esp32_wifi(gui_context.id_registry)

        model.viewport_rows = 3

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["m1", "m2", "m3"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        gui_context.registry.central.log_pool = pool

        model.prev_apply = 0
        model.apply_updates()
        assert model.row_count == 3

        count = 6
        messages = [f"m{i}" for i in range(1, count + 1)]
        bundle2 = make_bundle(
            timestamps=list(range(1, count + 1)),
            rx_timestamps=list(range(1, count + 1)),
            devices=[device.id] * count,
            levels=[0] * count,
            modules=[module.id] * count,
            sequences=list(range(1, count + 1)),
            messages=messages,
        )
        pool._segments = [FakeSegment(bundle2)]
        pool._latest_seq = count

        model.prev_apply = 0
        model.apply_updates()

        window = model.seq[model._valid_start : model._valid_start + model.row_count]
        assert model.row_count == 3
        assert list(window) == [4, 5, 6]
        assert np.unique(window).size == model.row_count

    def test_fetch_live_incremental_carries_message_cache_forward(self, model, gui_context):
        """The decoded-message cache must follow a carried row to its new slot in the
        now-active buffer, not go stale/empty and not leak some other row's cached text -
        regression test for _bind_active() no longer blindly nulling the whole cache every tick
        (see _fetch_live_incremental's new_cache carry-forward)."""
        device, module = esp32_wifi(gui_context.id_registry)

        model.viewport_rows = 3

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["first", "second", "third"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        gui_context.registry.central.log_pool = pool

        model.prev_apply = 0
        model.apply_updates()
        assert model.row_count == 3

        # Force every row's message to be decoded and cached at its current slot.
        assert model.get_cell(0, LogTableCol.MESSAGE) == "first"
        assert model.get_cell(1, LogTableCol.MESSAGE) == "second"
        assert model.get_cell(2, LogTableCol.MESSAGE) == "third"

        # One new row arrives - viewport is full, so this evicts "first" and carries
        # "second"/"third" forward into new slots in the other ping-ponged buffer.
        bundle2 = make_bundle(
            timestamps=[1, 2, 3, 4],
            rx_timestamps=[1, 2, 3, 4],
            devices=[device.id] * 4,
            levels=[0] * 4,
            modules=[module.id] * 4,
            sequences=[1, 2, 3, 4],
            messages=["first", "second", "third", "fourth"],
        )
        pool._segments = [FakeSegment(bundle2)]
        pool._latest_seq = 4

        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 3
        assert model.get_cell(0, LogTableCol.MESSAGE) == "second"
        assert model.get_cell(1, LogTableCol.MESSAGE) == "third"
        assert model.get_cell(2, LogTableCol.MESSAGE) == "fourth"

    def test_fetch_live_incremental_path_bounds_scan_to_new_rows_only(self, model, gui_context, monkeypatch):
        """A quiet tick (only a couple of new rows arrive) must be served by the incremental
        path, which reuses segment_filter_reversed but bounded by start_seq to only the rows
        added since the last fetch - not an unbounded rescan of the whole backend (which would
        pass start_seq=SEQ_NONE, the default). The whole point of this change is bounding scan
        cost to genuinely new activity."""
        device, module = esp32_wifi(gui_context.id_registry)

        big_count = 500
        big_bundle = make_bundle(
            timestamps=list(range(big_count)),
            rx_timestamps=list(range(big_count)),
            devices=[device.id] * big_count,
            levels=[0] * big_count,
            modules=[module.id] * big_count,
            sequences=list(range(1, big_count + 1)),
            messages=[f"row {i}" for i in range(big_count)],
        )
        pool = FakeLogPool(latest_seq=big_count, segments=[FakeSegment(big_bundle)])
        gui_context.registry.central.log_pool = pool

        model.prev_apply = 0
        model.apply_updates()  # primes the active buffer via the full-rescan (first-fetch) path
        assert model.row_count == model.viewport_rows

        import blinkview.core.log_fetch as log_fetch_module
        from blinkview.core.dtypes import SEQ_NONE

        reversed_spy = MagicMock(wraps=log_fetch_module.segment_filter_reversed)
        monkeypatch.setattr(log_fetch_module, "segment_filter_reversed", reversed_spy)

        # Only two new rows arrive - a "device mostly quiet" tick. Mirrors real segment growth
        # (the same active segment gains rows in place) rather than a brand-new segment.
        new_count = big_count + 2
        grown_bundle = make_bundle(
            timestamps=list(range(new_count)),
            rx_timestamps=list(range(new_count)),
            devices=[device.id] * new_count,
            levels=[0] * new_count,
            modules=[module.id] * new_count,
            sequences=list(range(1, new_count + 1)),
            messages=[f"row {i}" for i in range(new_count)],
        )
        pool._segments = [FakeSegment(grown_bundle)]
        pool._latest_seq = new_count
        model.prev_apply = 0
        model.apply_updates()

        reversed_spy.assert_called_once()
        _, call_kwargs = reversed_spy.call_args
        assert call_kwargs["start_seq"] == big_count  # bounded to "new since last tick", not SEQ_NONE
        assert call_kwargs["start_seq"] != SEQ_NONE
        assert model.row_count == model.viewport_rows
        window = model.seq[model._valid_start : model._valid_start + model.row_count]
        assert int(window[-1]) == big_count + 2

    def test_fetch_live_incremental_burst_keeps_newest_rows_not_oldest(self, model, gui_context):
        """When more new rows arrive since the last tick than fit in the viewport, the NEWEST
        ones must be kept (this is a live tail) - a naive forward/ascending scan capped at
        max_matches would instead keep the oldest of the new rows, which is wrong here."""
        device, module = esp32_wifi(gui_context.id_registry)

        model.viewport_rows = 20

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["m1", "m2", "m3"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        gui_context.registry.central.log_pool = pool

        model.prev_apply = 0
        model.apply_updates()
        assert model.row_count == 3

        # A burst of 300 new rows arrives in one tick - far more than the 20-row viewport.
        count = 306
        messages = [f"m{i}" for i in range(1, count + 1)]
        bundle2 = make_bundle(
            timestamps=list(range(1, count + 1)),
            rx_timestamps=list(range(1, count + 1)),
            devices=[device.id] * count,
            levels=[0] * count,
            modules=[module.id] * count,
            sequences=list(range(1, count + 1)),
            messages=messages,
        )
        pool._segments = [FakeSegment(bundle2)]
        pool._latest_seq = count

        model.prev_apply = 0
        model.apply_updates()

        window = model.seq[model._valid_start : model._valid_start + model.row_count]
        assert model.row_count == 20
        assert list(window) == list(range(count - 19, count + 1))

    def test_fetch_live_filter_change_forces_full_rescan_not_incremental(self, model, gui_context, monkeypatch):
        """Changing the filter must force a full rescan (previously-matched rows no longer
        reflect the new filter and can't be carried forward), even though reload_and_redraw()
        doesn't require the backend sequence to have moved."""
        device, module = esp32_wifi(gui_context.id_registry)

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["connection lost", "all good", "connection restored"],
        )
        gui_context.registry.central.log_pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])

        model.prev_apply = 0
        model.apply_updates()
        assert model.row_count == 3

        import blinkview.core.log_fetch as log_fetch_module

        reversed_spy = MagicMock(wraps=log_fetch_module.segment_filter_reversed)
        monkeypatch.setattr(log_fetch_module, "segment_filter_reversed", reversed_spy)

        model.log_filter.set_text_filter("connection")
        model.reload_and_redraw()

        reversed_spy.assert_called()
        assert model.row_count == 2
        assert model.get_cell(0, LogTableCol.MESSAGE) == "connection lost"
        assert model.get_cell(1, LogTableCol.MESSAGE) == "connection restored"

    def test_fetch_live_incremental_no_duplication_or_gaps_across_many_ticks(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)

        total = 0
        pool = FakeLogPool(latest_seq=0, segments=[])
        gui_context.registry.central.log_pool = pool

        for tick in range(8):
            added = (tick % 3) + 1
            total += added
            messages = [f"m{i}" for i in range(1, total + 1)]
            bundle = make_bundle(
                timestamps=list(range(1, total + 1)),
                rx_timestamps=list(range(1, total + 1)),
                devices=[device.id] * total,
                levels=[0] * total,
                modules=[module.id] * total,
                sequences=list(range(1, total + 1)),
                messages=messages,
            )
            pool._segments = [FakeSegment(bundle)]
            pool._latest_seq = total

            model.prev_apply = 0
            model.apply_updates()

            window = model.seq[model._valid_start : model._valid_start + model.row_count]
            expected_count = min(total, model.viewport_rows)
            assert model.row_count == expected_count
            assert list(window) == list(range(total - expected_count + 1, total + 1))
            assert np.unique(window).size == model.row_count

    def test_clear_logs_resets_both_double_buffers(self, model, gui_context):
        """After clear_logs(), a stale row from the buffer that was NOT active at clear time must
        not resurface via the incremental path's carry-over on a later tick."""
        device, module = esp32_wifi(gui_context.id_registry)

        bundle = make_bundle(
            timestamps=[1, 2, 3],
            rx_timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[0, 0, 0],
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["m1", "m2", "m3"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        gui_context.registry.central.log_pool = pool

        model.prev_apply = 0
        model.apply_updates()  # full rescan (flips into buffer 1)

        bundle2 = make_bundle(
            timestamps=[1, 2, 3, 4],
            rx_timestamps=[1, 2, 3, 4],
            devices=[device.id] * 4,
            levels=[0, 0, 0, 0],
            modules=[module.id] * 4,
            sequences=[1, 2, 3, 4],
            messages=["m1", "m2", "m3", "m4"],
        )
        pool._segments = [FakeSegment(bundle2)]
        pool._latest_seq = 4
        model.prev_apply = 0
        model.apply_updates()  # incremental (flips back into buffer 0, carrying rows forward)

        model.clear_logs()
        assert model.row_count == 0

        bundle3 = make_bundle(
            timestamps=[10, 11],
            rx_timestamps=[10, 11],
            devices=[device.id] * 2,
            levels=[0, 0],
            modules=[module.id] * 2,
            sequences=[10, 11],
            messages=["fresh1", "fresh2"],
        )
        pool._segments = [FakeSegment(bundle3)]
        pool._latest_seq = 11
        model.prev_apply = 0
        model.apply_updates()

        assert model.row_count == 2
        assert model.get_cell(0, LogTableCol.MESSAGE) == "fresh1"
        assert model.get_cell(1, LogTableCol.MESSAGE) == "fresh2"

    def test_set_viewport_rows_growth_beyond_original_capacity_reallocates(self, model, gui_context):
        original_capacity = model.capacity
        big_rows = original_capacity + 50

        model.set_viewport_rows(big_rows)

        assert model.viewport_rows == big_rows
        assert model.capacity >= big_rows
        for batch in model._batches:
            assert batch.bundle.capacity >= big_rows
        assert len(model._identity_indices) == model.capacity


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
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=20)

        model.enter_history_mode(anchor_seq=10)

        assert model.mode == LogViewMode.HISTORY
        assert model.row_count == 20  # small backend: entire history fits in the before+after window
        # Rows should be in ascending chronological order (message m0..m19 for seq 1..20).
        first_msg = model.get_cell(0, LogTableCol.MESSAGE)
        last_msg = model.get_cell(model.row_count - 1, LogTableCol.MESSAGE)
        assert first_msg == "m0"
        assert last_msg == "m19"

    def test_enter_history_mode_with_anchor_ts_fetches_before_and_after(self, model, gui_context):
        """Timestamp counterpart to anchor_seq, used by LogTableViewerWidget's playback-clock
        following: end_ts is an inclusive upper bound for "before", start_ts an inclusive lower
        bound for "after" (see LogSegmentScanner.scan_history_window's docstring)."""
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=20)  # timestamps 0..19, seqs 1..20

        model.enter_history_mode(anchor_ts=10)

        assert model.mode == LogViewMode.HISTORY
        assert model.anchor_seq is None
        assert model.anchor_ts == 10
        # "before": ts <= 9 (m0..m9); "after": ts >= 10 (m10..m19).
        assert model.row_count == 20
        assert model.get_cell(0, LogTableCol.MESSAGE) == "m0"
        assert model.get_cell(9, LogTableCol.MESSAGE) == "m9"
        assert model.get_cell(10, LogTableCol.MESSAGE) == "m10"
        assert model.get_cell(model.row_count - 1, LogTableCol.MESSAGE) == "m19"

    def test_anchor_scroll_row_ts_anchored_returns_before_after_boundary(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=20)

        model.enter_history_mode(anchor_ts=10)

        # 10 rows (m0..m9) fall in the "before" region - the boundary row is index 10.
        assert model.anchor_scroll_row() == 10

    def test_anchor_scroll_row_seq_anchored_matches_row_for_seq(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=20)

        model.enter_history_mode(anchor_seq=10)

        assert model.anchor_scroll_row() == model.row_for_seq(10)

    def test_reload_and_redraw_preserves_anchor_ts(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=20)

        model.enter_history_mode(anchor_ts=10)
        model.log_filter.set_level(LogLevel.ERROR.name_conf)  # forces a reload
        model.reload_and_redraw()

        assert model.mode == LogViewMode.HISTORY
        assert model.anchor_ts == 10

    def test_history_mode_anchor_at_first_message_has_no_before_rows(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=5)

        model.enter_history_mode(anchor_seq=1)

        assert model.mode == LogViewMode.HISTORY
        # Nothing exists before sequence 1, so the window should start exactly at the anchor.
        assert model.seq_for_row(0) == 1

    def test_history_mode_honors_kv_filter_in_both_before_and_after_scans(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)

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

        assert model.mode == LogViewMode.HISTORY
        assert model.row_count == 10  # only the even-indexed (status=ok) rows survive
        for row in range(model.row_count):
            assert "status=ok" in model.get_cell(row, LogTableCol.MESSAGE)

    def test_history_mode_honors_backend_text_filter_in_both_before_and_after_scans(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)

        messages = ["needle" if i % 2 == 0 else "noise" for i in range(20)]
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

        model.log_filter.set_text_filter("needle")
        model.enter_history_mode(anchor_seq=10)

        assert model.mode == LogViewMode.HISTORY
        assert model.row_count == 10  # only the even-indexed ("needle") rows survive
        for row in range(model.row_count):
            assert model.get_cell(row, LogTableCol.MESSAGE) == "needle"

    def test_enter_live_mode_resets_anchor_and_refetches(self, model, gui_context):
        device, module = esp32_wifi(gui_context.id_registry)
        self._make_pool(gui_context, device, module, count=5)

        model.enter_history_mode(anchor_seq=3)
        assert model.mode == LogViewMode.HISTORY

        model.enter_live_mode()

        assert model.mode == LogViewMode.LIVE
        assert model.anchor_seq is None
        assert model.row_count == 5  # small backend: live view shows everything that exists


class TestLogTableCanvas:
    """Exercises LogTableCanvas (the QAbstractScrollArea-based direct-paint replacement for
    QTableView) - none of the LogTableStore tests above touch this class at all, since it's the
    Qt widget layer painting from the store, not the store itself."""

    def _make_canvas(self, qtbot, model, gui_context, rows):
        device, module = esp32_wifi(gui_context.id_registry)
        model.row_count = len(rows)
        for i, (seq, message) in enumerate(rows):
            _populate_row(model, i, ts=i, dev_id=device.id, level=0, mod_id=module.id, seq=seq, message=message)
        canvas = LogTableCanvas(model)
        qtbot.addWidget(canvas)
        canvas.resize(400, 200)
        return canvas

    def test_paint_event_runs_without_error(self, qtbot, model, gui_context):
        from qtpy.QtGui import QPaintEvent

        canvas = self._make_canvas(qtbot, model, gui_context, [(1, "first"), (2, "second"), (3, "third")])
        canvas.paintEvent(QPaintEvent(canvas.viewport().rect()))  # must not raise

    def test_row_at_hit_tests_header_and_rows_correctly(self, qtbot, model, gui_context):
        from qtpy.QtCore import QPoint

        from blinkview.ui.widgets.log_table_viewer import HEADER_HEIGHT, ROW_HEIGHT

        canvas = self._make_canvas(qtbot, model, gui_context, [(1, "first"), (2, "second"), (3, "third")])
        assert canvas._row_at(QPoint(10, 5)) is None  # inside the header strip
        assert canvas._row_at(QPoint(10, HEADER_HEIGHT + 1)) == 0
        assert canvas._row_at(QPoint(10, HEADER_HEIGHT + ROW_HEIGHT + 1)) == 1
        assert canvas._row_at(QPoint(10, 10_000)) is None  # far below the last row

    def test_mouse_press_selects_row_by_seq(self, qtbot, model, gui_context):
        from qtpy.QtCore import QPoint, Qt

        from blinkview.ui.widgets.log_table_viewer import HEADER_HEIGHT, ROW_HEIGHT

        canvas = self._make_canvas(qtbot, model, gui_context, [(1, "first"), (2, "second")])
        pos = QPoint(10, HEADER_HEIGHT + ROW_HEIGHT + 1)  # second visible row

        qtbot.mouseClick(canvas.viewport(), Qt.LeftButton, pos=pos)

        assert canvas.selected_seq == 2

    def test_wheel_up_while_live_triggers_callback_not_default_scroll(self, qtbot, model, gui_context):
        class FakeDelta:
            def y(self):
                return 120

        class FakeWheelEvent:
            def angleDelta(self):
                return FakeDelta()

            def accept(self):
                pass

        canvas = self._make_canvas(qtbot, model, gui_context, [(1, "first")])
        model.mode = LogViewMode.LIVE
        triggered = []
        canvas.on_wheel_up_while_live = lambda: triggered.append(True)

        canvas.wheelEvent(FakeWheelEvent())

        assert triggered == [True]

    def test_autosize_columns_measures_visible_row_content(self, qtbot, model, gui_context):
        canvas = self._make_canvas(
            qtbot, model, gui_context, [(1, "short"), (2, "a much, much longer message than the others")]
        )
        canvas.autosize_columns()
        assert canvas._col_width[LogTableCol.TIMESTAMP] > 0
        assert canvas._col_width[LogTableCol.MESSAGE] > 0

    def test_set_column_visible_recomputes_message_stretch_width(self, qtbot, model, gui_context):
        canvas = self._make_canvas(qtbot, model, gui_context, [(1, "hello")])
        before = canvas._col_width[LogTableCol.MESSAGE]

        canvas.set_column_visible(LogTableCol.RX_TIMESTAMP, True)

        assert LogTableCol.RX_TIMESTAMP in canvas._visible_columns()
        # MESSAGE stretches to whatever remains - showing another column should shrink it.
        assert canvas._col_width[LogTableCol.MESSAGE] <= before
