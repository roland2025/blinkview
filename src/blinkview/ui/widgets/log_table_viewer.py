# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from enum import IntEnum
from time import time_ns
from typing import TYPE_CHECKING, Optional

import numpy as np
from qtpy.QtCore import QRect, Qt, QTimer
from qtpy.QtGui import QAction, QColor, QFontMetrics, QPainter
from qtpy.QtWidgets import (
    QAbstractScrollArea,
    QActionGroup,
    QApplication,
    QComboBox,
    QLineEdit,
    QMenu,
    QSplitter,
    QToolBar,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.log_fetch import LogSegmentScanner
from blinkview.core.module_snapshot import MAX_MSG_BYTES
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.playback_follow import (
    ClockSnapshot,
    FollowActionKind,
    FollowEvent,
    FollowState,
    PlaybackFollowMachine,
)
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.formatting import nb_format_local_timestamp
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS
from blinkview.ops.segments import nb_segment_extract_fields, segment_filter_reversed
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH
from blinkview.ui.constants import WidgetName
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.log_velocity_tracker import LogVelocityTracker
from blinkview.ui.widget_registry import register_widget_factory
from blinkview.ui.widgets.kv_filter_line_edit import KvFilterLineEdit
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from blinkview.utils.utc_offset import get_local_utc_offset_seconds

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

ROW_HEIGHT = 16
HEADER_HEIGHT = 22

# Column auto-sizing throttle for columns whose content can keep changing width (MODULE/PROCESS
# names) - re-measuring on every single paint (live mode ticks at ~10Hz) would be wasteful.
COLUMN_AUTOSIZE_THROTTLE_NS = 2_000_000_000

# LogLevel.LIST is a small fixed set - precompute QColor/name lookups once at import time instead
# of allocating a new QColor (and re-resolving LogLevel.from_value) on every single cell paint.
# Keyed by the raw int value (dtypes.LEVEL_TYPE), same as LogLevel.DICT.
_LEVEL_COLORS = {level.value: QColor(level.color) for level in LogLevel.LIST}
_LEVEL_NAMES = {level.value: level.name_conf for level in LogLevel.LIST}


class LogTableCol(IntEnum):
    TIMESTAMP = 0
    RX_TIMESTAMP = 1
    DEVICE = 2
    LEVEL = 3
    PROCESS = 4
    THREAD = 5
    MODULE = 6
    MESSAGE = 7


_COLUMN_LABELS = {
    LogTableCol.TIMESTAMP: "Time",
    LogTableCol.RX_TIMESTAMP: "Rx Time",
    LogTableCol.DEVICE: "Device",
    LogTableCol.LEVEL: "Level",
    LogTableCol.MODULE: "Module",
    LogTableCol.MESSAGE: "Message",
    LogTableCol.PROCESS: "Process",
    LogTableCol.THREAD: "Thread",
}


class LogTableStore:
    """Two-tier structured (columnar) log row store, filtered by a shared LogFilter the same
    way LogViewerWidget filters its text stream:

    - LIVE mode: fetches only as many of the most recent matching rows as fit the visible
      viewport (~viewport_rows, no held-back scrollback), re-fetched whenever the backend's
      sequence counter advances. No scrollbar is needed since there's nothing to scroll to.
    - HISTORY mode: entered when the user scrolls away from the live tail. Fetches a bounded
      window of rows immediately before and after an anchor sequence (the row that was on top
      when history mode was entered), so the user can scroll through a couple hundred rows of
      context in either direction without the model holding unbounded scrollback. This window
      is static (not refreshed on new data) until the widget returns to live mode.

    Plain Python object (not a QAbstractTableModel/QObject) - LogTableCanvas paints directly
    from this store's arrays instead of going through Qt's model/view/delegate machinery, which
    was found to force a full-viewport repaint on every single content change regardless of how
    cheap data() itself was made (see qt-log-table-viewer skill). Callers are responsible for
    triggering a repaint themselves (LogTableCanvas.request_repaint()) after any call here that
    changes row_count/content - there are no Qt signals to do it automatically anymore.
    """

    HISTORY_BEFORE = 300
    HISTORY_AFTER = 300

    def __init__(self, gui_context, log_filter: LogFilter, filter_sidebar):
        self.gui_context: GUIContext = gui_context
        self.log_filter = log_filter
        self.filter_sidebar = filter_sidebar

        self._scanner = LogSegmentScanner(
            self.gui_context.id_registry,
            lambda: self.gui_context.registry.central.log_pool,
            self.log_filter,
            get_sidebar_filter=self.filter_sidebar.get_filter,
            get_show_hidden=self.filter_sidebar.action_show_non_essential.isChecked,
        )

        self.viewport_rows = 100  # LIVE mode fetch bound; the widget keeps this in sync with the view's height

        self.capacity = max(self.viewport_rows, self.HISTORY_BEFORE + self.HISTORY_AFTER)

        self.mode = LogViewMode.LIVE
        self.anchor_seq = None
        # Timestamp counterpart to anchor_seq, mutually exclusive with it - set when the current
        # history window was anchored on the global registry.playback_clock's virtual time
        # rather than a manually-scrolled/paused sequence id (see LogTableViewerWidget's
        # follow_playback wiring).
        self.anchor_ts = None
        # The "before" region's row count from the most recent _fetch_history call - the
        # boundary row index between the before/after regions, needed to reposition the
        # scrollbar for a ts-anchored window (there's no single matching seq to search for the
        # way row_for_seq(anchor_seq) finds one for a seq-anchored window).
        self._before_count = 0

        # row_count rows are valid, starting at self._valid_start within the fixed-size arrays
        # below (avoids ever having to shift/copy data to "row 0" after a rebuild).
        self.row_count = 0
        self._valid_start = self.capacity

        # Set to True whenever apply_updates() actually ran a fetch that changed row content -
        # False when throttled, the backend sequence didn't move, or no row matched the filter.
        # LogTableViewerWidget checks this after every apply_updates() call to decide whether a
        # repaint is even worth scheduling, so a quiet live tail doesn't repaint at the full
        # heartbeat rate for no reason.
        self.last_fetch_changed = False

        # How many new rows the last apply_updates() fetch matched (0 if throttled, no match, or
        # not in LIVE mode) - LogTableViewerWidget feeds this into its LogVelocityTracker the same
        # way LogViewerWidget feeds total_new_rows, to decide whether to auto-pause into history.
        self.last_fetch_new_rows = 0

        # Two PooledLogBatch buffers, ping-ponged by _bind_active()/self._active. LIVE mode's
        # incremental fetch (_fetch_live_incremental) writes the next tick's rows into the
        # currently-INACTIVE buffer (carrying forward matches already sitting in the active one)
        # so the buffer the view is currently reading from is never mutated out from under it;
        # HISTORY mode isn't per-tick so it just writes in place into whichever is active.
        self._batches = []
        self._buf_valid_start = []
        self._buf_row_count = []
        self._array_pool = self.gui_context.registry.system_ctx.array_pool
        for _ in range(2):
            self._batches.append(self._alloc_batch(self.capacity))
            self._buf_valid_start.append(self.capacity)
            self._buf_row_count.append(0)
        self._active = 0

        # identity_indices[i] == i by construction, so any contiguous slice is exactly the
        # "indices" array nb_segment_extract_fields needs to copy those same-numbered rows out of
        # a source buffer - lets the incremental fetch's carry-over step reuse that kernel with
        # zero per-tick allocation.
        self._identity_indices = np.arange(self.capacity, dtype=np.int64)

        self._message_cache = [None] * self.capacity
        self._bind_active()

        # Scratch buffer for nb_format_local_timestamp - reused across every _format_ts call
        # instead of allocating a fresh array per cell paint (this runs once per visible
        # TIMESTAMP/RX_TIMESTAMP cell on every repaint). _ts_scratch_mv is a cached memoryview
        # over the same buffer - slicing/decoding through it avoids re-wrapping a numpy array
        # (with its dtype/shape checks) on every single cell.
        self._ts_scratch = np.empty(18, dtype=np.uint8)
        self._ts_scratch_mv = memoryview(self._ts_scratch)

        self._tz_offset_ns = get_local_utc_offset_seconds() * 1_000_000_000
        self.ts_precision = 3  # 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds

        # Flat id -> name caches for the DEVICE/MODULE columns, used instead of a dict .get() +
        # attribute lookup through gui_context.id_registry on every single cell paint. Relies on
        # IDRegistry assigning ids sequentially from 0 (see registry.py's _device_id_counter/
        # _module_id_counter) so a plain list indexed by id is valid and append-only growable -
        # same assumption IDRegistry._essential_array already relies on. Names are immutable once
        # registered (registry.py only ever adds new devices/modules, never renames), so entries
        # never need invalidating, only appending to.
        self._device_name_cache = []
        self._module_name_cache = []

        # Sentinel forces a rebuild on the very first live tick regardless of the backend's sequence.
        self._last_backend_seq = None
        self.prev_apply = 0

        # Identity of the effective_mask/kv/text used by the *previous* successful fetch - used to
        # decide whether the next fetch may take the cheap incremental path (forward-scan only
        # rows added since last time, carrying forward previously-matched rows) or must fall back
        # to a full rescan (the filter itself changed, so previously-matched rows are no longer
        # valid). bake_kv_arrays()/bake_text_search()/LogSegmentScanner.ensure_effective_mask()
        # all return the same object instance until something in them actually changes, so `is`
        # comparisons are enough.
        self._prev_fetch_mask = None
        self._prev_fetch_kv = None
        self._prev_fetch_text = None

        self.logger = self.gui_context.logger.child("log_table", enabled=True)
        self.logger_fetch = self.logger.child("fetch")

    def _alloc_batch(self, capacity: int) -> PooledLogBatch:
        return self._array_pool.create(
            PooledLogBatch,
            capacity,
            capacity * MAX_MSG_BYTES,
            has_levels=True,
            has_modules=True,
            has_devices=True,
            has_sequences=True,
            has_pids=True,
            has_tids=True,
        )

    def _bind_active(self):
        """Re-points the cached column attrs at whichever buffer is currently active. Called on
        construction and every time a fetch flips self._active. Plain rebound instance attributes
        (not properties) since callers - including the test harness - expect self.ts/self.dev/etc.
        to be plain writable numpy array views, and nothing needs read/write interception.

        Also rebinds a *_mv memoryview twin of each scalar-column array, used only by the paint
        path's per-cell hot loop: indexing a memoryview of a fixed-width dtype yields a plain
        Python int/float directly, skipping both the np.int64/uint32 scalar-object wrapper numpy
        array indexing allocates and a subsequent int(...) unwrap. Code that needs real numpy
        semantics (slicing, ==, kernels) keeps using the plain array attrs above - memoryviews
        don't support that.

        Deliberately does NOT touch self._message_cache - unlike the array attrs above (which are
        always safe to blindly rebind, since a stale numpy view is just replaced), a slot's cached
        decoded string is only valid for the specific bytes currently occupying that slot in
        whichever buffer is now active, so blindly nulling the whole cache here would throw away
        still-valid decodes for carried-forward rows every single live tick. Every caller is
        responsible for setting self._message_cache itself, appropriately for what it just wrote
        (_fetch_live_incremental carries forward the entries for rows it carried forward; every
        other caller here rewrites the whole buffer and must reset the whole cache)."""
        bundle = self._batches[self._active].bundle
        self.ts = bundle.timestamps
        self.rx_ts = bundle.rx_timestamps
        self.dev = bundle.devices
        self.lvl = bundle.levels
        self.mod = bundle.modules
        self.seq = bundle.sequences
        self.pid = bundle.pids
        self.tid = bundle.tids
        self.msg_offsets = bundle.offsets
        self.msg_buffer = bundle.buffer
        self.msg_lengths = bundle.lengths

        self.ts_mv = memoryview(self.ts)
        self.rx_ts_mv = memoryview(self.rx_ts)
        self.dev_mv = memoryview(self.dev)
        self.lvl_mv = memoryview(self.lvl)
        self.mod_mv = memoryview(self.mod)
        self.pid_mv = memoryview(self.pid)
        self.tid_mv = memoryview(self.tid)
        self.msg_offsets_mv = memoryview(self.msg_offsets)
        self.msg_lengths_mv = memoryview(self.msg_lengths)

        self._valid_start = self._buf_valid_start[self._active]
        self.row_count = self._buf_row_count[self._active]

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Exercises nb_segment_extract_fields - the columnar-extraction kernel unique to this
        class. segment_filter_reversed/nb_filter_segment (every start_seq/end_seq shape
        _fetch_live/_fetch_history use) are now warmed once by LogSegmentScanner.warmup
        (core/log_fetch.py) instead of duplicated here - both callbacks are registered
        independently and NumbaWarmupHelper.run_all() runs every registered callback, so no
        explicit ordering/dependency between them is needed."""
        print("[Warmup] LogTableStore ...")

        # _format_ts's per-cell-paint kernel - precision=9 alone compiles every branch (branches
        # aren't literal-specialized on a runtime bool/int, see numba-njit skill §3).
        nb_format_local_timestamp(np.empty(18, dtype=np.uint8), 0, 0, 9)

        capacity = 8
        max_msg_bytes = 64
        effective_mask = np.zeros(max(10, helper.registry.module_count()), dtype=dtypes.LEVEL_TYPE)

        with (
            helper.array_pool.create(
                PooledLogBatch,
                capacity,
                capacity * max_msg_bytes,
                has_levels=True,
                has_modules=True,
                has_devices=True,
                has_sequences=True,
                has_pids=True,
                has_tids=True,
            ) as out_batch,
            helper.log_pool.get_reversed_snapshot() as segments,
            helper.log_pool.acquire_indices_buffer() as indices,
        ):
            for segment in segments:
                if segment.size == 0:
                    continue

                match_count = segment_filter_reversed(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=capacity,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )

                if match_count > 0:
                    nb_segment_extract_fields(
                        segment.bundle,
                        indices.array,
                        match_count,
                        out_batch.bundle,
                        0,
                        max_msg_bytes,
                    )
                break
                break

        print("[Warmup] LogTableStore ... done")

    # --- Row access ------------------------------------------------------------------------

    def _row_to_slot(self, row: int) -> int:
        """Maps a model row (0 = oldest currently shown) to its slot in the fixed-size arrays."""
        return self._valid_start + row

    def _format_ts(self, ts_ns: int) -> str:
        length = nb_format_local_timestamp(self._ts_scratch, ts_ns, self._tz_offset_ns, self.ts_precision)
        return self._ts_scratch_mv[:length].tobytes().decode("ascii")

    def set_ts_precision(self, precision: int):
        if self.ts_precision == precision:
            return
        self.ts_precision = precision

    def get_color(self, row: int) -> Optional[QColor]:
        if row < 0 or row >= self.row_count:
            return None
        slot = self._row_to_slot(row)
        return _LEVEL_COLORS.get(self.lvl_mv[slot])

    def get_cell(self, row: int, col: LogTableCol) -> Optional[str]:
        if row < 0 or row >= self.row_count:
            return None

        slot = self._row_to_slot(row)

        if col == LogTableCol.TIMESTAMP:
            return self._format_ts(self.ts_mv[slot])
        if col == LogTableCol.RX_TIMESTAMP:
            return self._format_ts(self.rx_ts_mv[slot])
        if col == LogTableCol.DEVICE:
            return self._device_name(self.dev_mv[slot])
        if col == LogTableCol.LEVEL:
            return _LEVEL_NAMES.get(self.lvl_mv[slot], "?")
        if col == LogTableCol.MODULE:
            return self._module_name(self.mod_mv[slot])
        if col == LogTableCol.MESSAGE:
            return self._decode_message(slot)
        if col == LogTableCol.PROCESS:
            pid = self.pid_mv[slot]
            if pid == 0:
                return "-"
            key = (self.dev_mv[slot] << 32) | pid
            name = self.gui_context.registry.pid_history.resolve(key, self.ts_mv[slot])
            return name if name else "-"
        if col == LogTableCol.THREAD:
            tid = self.tid_mv[slot]
            return str(tid) if tid else "-"

        return None

    def seq_for_row(self, row: int):
        """Returns the backend sequence id for a model row, used by the widget to anchor
        history-mode fetches and relocate the viewport after a rebuild."""
        if row < 0 or row >= self.row_count:
            return None
        return int(self.seq[self._row_to_slot(row)])

    def row_for_seq(self, target_seq) -> int:
        """Reverse of seq_for_row: finds the row currently holding a given sequence id, or -1."""
        if target_seq is None or self.row_count == 0:
            return -1
        window = self.seq[self._valid_start : self._valid_start + self.row_count]
        matches = np.nonzero(window == target_seq)[0]
        return int(matches[0]) if matches.size > 0 else -1

    def _decode_message(self, slot: int) -> str:
        cached = self._message_cache[slot]
        if cached is not None:
            return cached

        msg_len = self.msg_lengths_mv[slot]
        off = self.msg_offsets_mv[slot]
        text = self.msg_buffer[off : off + msg_len].tobytes().decode("utf-8", errors="replace")
        self._message_cache[slot] = text
        return text

    def _device_name(self, dev_id: int) -> str:
        cache = self._device_name_cache
        if dev_id >= len(cache):
            devices = self.gui_context.id_registry.devices
            for i in range(len(cache), len(devices)):
                identity = devices.get(i)
                cache.append(identity.name if identity else "?")
        return cache[dev_id] if dev_id < len(cache) else "?"

    def _module_name(self, mod_id: int) -> str:
        cache = self._module_name_cache
        if mod_id >= len(cache):
            modules = self.gui_context.id_registry.modules
            for i in range(len(cache), len(modules)):
                identity = modules.get(i)
                cache.append(identity.name if identity else "?")
        return cache[mod_id] if mod_id < len(cache) else "?"

    # --- Mode management ------------------------------------------------------------------

    def set_viewport_rows(self, n: int):
        """Called by the widget when the view is resized, so LIVE mode fetches exactly enough
        rows to fill the visible area - never more, since LIVE mode has no scrollbar to reach
        any rows beyond what fits on screen."""
        n = max(1, n)
        if n == self.viewport_rows:
            return
        self.viewport_rows = n
        if n > self.capacity:
            self._grow_capacity(n)
        if self.mode == LogViewMode.LIVE:
            self._last_backend_seq = None  # force a refetch at the new size
            self._fetch_live(force=True)

    def _grow_capacity(self, min_capacity: int):
        """Reallocates both buffers (and the identity-index scratch array) large enough to hold
        min_capacity rows. viewport_rows growing past the original capacity (e.g. resizing to a
        very tall view) would otherwise make the fetch write past the end of the fixed-size
        arrays - a silent negative-index wraparound, not a crash. Old buffer contents don't map
        cleanly onto a larger backing array, so both buffers' bookkeeping resets to empty; the
        caller always forces a full refetch immediately after (set_viewport_rows always does)."""
        self.capacity = max(min_capacity, self.HISTORY_BEFORE + self.HISTORY_AFTER)
        self._batches = [self._alloc_batch(self.capacity) for _ in range(2)]
        self._buf_valid_start = [self.capacity, self.capacity]
        self._buf_row_count = [0, 0]
        self._identity_indices = np.arange(self.capacity, dtype=np.int64)
        self._active = 0
        self._bind_active()
        self._message_cache = [None] * self.capacity

    def enter_live_mode(self):
        self.mode = LogViewMode.LIVE
        self.anchor_seq = None
        self.anchor_ts = None
        self._last_backend_seq = None
        self._fetch_live(force=True)

    def enter_history_mode(self, anchor_seq=None, anchor_ts=None):
        """anchor_seq/anchor_ts are mutually exclusive: a sequence id for manual scroll/pause
        paging, or a timestamp for playback-clock following (see LogTableViewerWidget's
        follow_playback wiring)."""
        if anchor_seq is None and anchor_ts is None:
            return
        self.mode = LogViewMode.HISTORY
        self.anchor_seq = anchor_seq
        self.anchor_ts = anchor_ts
        self._scanner.ensure_effective_mask()
        self._fetch_history(anchor_seq=anchor_seq, anchor_ts=anchor_ts)

    def anchor_scroll_row(self):
        """The row index the view should be scrolled to right after a history-mode fetch. A
        seq-anchored window scrolls to the exact row holding that sequence id; a ts-anchored
        window has no single matching seq to search for, so it scrolls to the before/after
        boundary instead (mirrors LogViewerWidget._reanchor_history's
        text_area.scroll_to_block(before_count))."""
        if self.anchor_seq is not None:
            return self.row_for_seq(self.anchor_seq)
        return self._before_count

    def clear_logs(self):
        self._buf_valid_start = [self.capacity, self.capacity]
        self._buf_row_count = [0, 0]
        self._bind_active()
        self._message_cache = [None] * self.capacity
        log_pool = self.gui_context.registry.central.log_pool
        self._last_backend_seq = log_pool.latest_sequence()
        self.mode = LogViewMode.LIVE
        self.anchor_seq = None
        self.anchor_ts = None

    def reload_and_redraw(self):
        """Forces a re-fetch under the current mode (used when filter settings change - same
        data, different criteria). Stays anchored to the same position in history mode."""
        self._scanner.invalidate_mask()
        if self.mode == LogViewMode.HISTORY and (self.anchor_seq is not None or self.anchor_ts is not None):
            self._scanner.ensure_effective_mask()
            self._fetch_history(anchor_seq=self.anchor_seq, anchor_ts=self.anchor_ts)
        else:
            self._last_backend_seq = None
            self._fetch_live(force=True)

    # --- LIVE mode fetch --------------------------------------------------------------------

    def apply_updates(self):
        """Driven by the GUIContext heartbeat (~10Hz, or slower for a large viewport - see
        _live_throttle_ns). Only does anything in LIVE mode - HISTORY mode is a static snapshot
        until the widget calls enter_live_mode()/enter_history_mode() again in response to
        scrolling."""
        self.last_fetch_changed = False
        self.last_fetch_new_rows = 0

        if self.mode != LogViewMode.LIVE:
            return

        now = time_ns()
        if now - self.prev_apply < self._live_throttle_ns():
            return
        self.prev_apply = now

        self._fetch_live()

    def _live_throttle_ns(self) -> int:
        """LIVE mode's per-tick cost used to be dominated by Qt's own repaint (QAbstractItemView
        always repaints the entire viewport on any content change, confirmed empirically - see
        qt-log-table-viewer skill), which is why this scales with viewport_rows. Now that painting
        goes through LogTableCanvas's own direct paintEvent instead of QTableView, the per-cell
        cost is much lower, but the scaling throttle is kept anyway - repainting more of the
        viewport is still strictly more work than repainting less of it, regardless of how cheap
        each cell became, and there's no reason to burn cycles redrawing a few hundred rows at the
        same ~10Hz rate a two-dozen-row window uses."""
        baseline_rows = 50
        base_interval_ns = 100_000_000
        return base_interval_ns * max(1, self.viewport_rows // baseline_rows)

    def _fetch_live(self, force: bool = False):
        """Dispatches to a full backward rescan (_fetch_live_full) or a cheap incremental
        forward-scan-and-carry (_fetch_live_incremental), depending on whether a full rescan is
        actually required this tick. Both write into the currently-INACTIVE buffer and flip
        self._active on success, so the buffer the view is reading from is never mutated
        mid-read."""
        pool = self.gui_context.registry.central.log_pool
        current_backend_seq = pool.latest_sequence()

        if not force and current_backend_seq == self._last_backend_seq:
            return  # Nothing new in the backend since the last fetch - skip entirely.

        self.filter_sidebar.sync_modules()
        self._scanner.ensure_effective_mask()
        kv = self.log_filter.bake_kv_arrays()
        text = self.log_filter.bake_text_search()

        # A full rescan is unavoidable on the very first fetch, whenever the caller explicitly
        # forces one (viewport resize, mode transitions, clear), or when the filter itself
        # changed since the last successful fetch - in that last case, whatever rows are
        # currently sitting in the active buffer no longer reflect the filter that would produce
        # them, so they can't be carried forward. Otherwise, the incremental path is strictly
        # cheaper and correct: it only has to look at rows added since self._last_backend_seq.
        filter_changed = (
            self._prev_fetch_mask is not self._scanner.effective_mask
            or self._prev_fetch_kv is not kv
            or self._prev_fetch_text is not text
        )

        if force or self._last_backend_seq is None or filter_changed:
            self.last_fetch_changed = True
            self._fetch_live_full()
        else:
            self._fetch_live_incremental()

        self._last_backend_seq = current_backend_seq
        self._prev_fetch_mask = self._scanner.effective_mask
        self._prev_fetch_kv = kv
        self._prev_fetch_text = text

    def _fetch_live_full(self):
        """Full backward rescan across the entire backend, capped at viewport_rows matches -
        the only correct option when there's no valid "previous fetch" to carry forward from
        (first fetch, forced refetch, or the filter itself changed). Delegates segment iteration
        to LogSegmentScanner.scan_tail (start_seq=SEQ_NONE - unbounded, matching this method's
        original "scan every segment, no watermark" loop)."""
        inactive_idx = 1 - self._active
        inactive = self._batches[inactive_idx]

        write_cursor = self.capacity

        def _consume(segment, indices_array, match_count):
            nonlocal write_cursor
            write_cursor -= match_count
            nb_segment_extract_fields(
                segment.bundle, indices_array, match_count, inactive.bundle, write_cursor, MAX_MSG_BYTES
            )

        t_start = time_ns()
        result = self._scanner.scan_tail(start_seq=SEQ_NONE, max_rows=self.viewport_rows, consume=_consume)
        total_new = result.total_new_rows

        self.logger_fetch.debug(f"live full {(time_ns() - t_start) / 1_000_000:.3f} ms | rows={total_new}")

        self._buf_valid_start[inactive_idx] = write_cursor
        self._buf_row_count[inactive_idx] = total_new
        self._active = inactive_idx
        self._bind_active()
        self._message_cache = [None] * self.capacity
        self.last_fetch_new_rows = total_new

    def _fetch_live_incremental(self):
        """Backward-scans only rows newer than self._last_backend_seq (bounded via the same
        start_seq mechanism _fetch_history's "before" scan uses), then carries forward the
        currently-active buffer's own newest rows to fill out whatever's left of the viewport -
        bounds scan cost to genuinely new backend activity instead of total backend depth, so a
        filter that rarely matches (or a device that's gone quiet) costs almost nothing per tick.

        Scanning backward (not forward) is what makes this correct for a burst tick: if more than
        `limit` new rows match since the last fetch, we need the NEWEST `limit` of them (this is
        a live tail), and a backward scan capped at max_matches finds exactly those first, whereas
        a forward/ascending scan capped the same way would find the OLDEST new matches instead -
        the wrong end for a live view."""
        active_idx = self._active
        inactive_idx = 1 - active_idx
        active = self._batches[active_idx]
        inactive = self._batches[inactive_idx]

        limit = self.viewport_rows
        start_seq = self._last_backend_seq

        write_cursor = self.capacity

        def _consume(segment, indices_array, match_count):
            nonlocal write_cursor
            write_cursor -= match_count
            nb_segment_extract_fields(
                segment.bundle, indices_array, match_count, inactive.bundle, write_cursor, MAX_MSG_BYTES
            )

        t_start = time_ns()
        result = self._scanner.scan_tail(start_seq=start_seq, max_rows=limit, consume=_consume)
        total_new = result.total_new_rows

        if total_new == 0:
            # Nothing actually matched since the last fetch (the backend sequence advanced but
            # no new row passed the filter) - the active buffer's content is still exactly
            # correct, so skip the flip entirely rather than doing a no-op buffer swap.
            # last_fetch_changed stays False - the widget won't bother repainting.
            self.logger_fetch.debug("live incr 0.000 ms | new=0 carried=0 (skipped)")
            return

        self.last_fetch_changed = True

        # The new rows already sit right-aligned to self.capacity (write_cursor..capacity). Carry
        # forward as many of the active buffer's own newest rows as fit in what's left of the
        # viewport, landing immediately to their left - the two regions are disjoint by
        # construction (carried rows all have seq <= start_seq, new rows all have seq > start_seq)
        # so this never duplicates or drops a row.
        active_valid_start = self._buf_valid_start[active_idx]
        active_row_count = self._buf_row_count[active_idx]
        carry_count = min(active_row_count, limit - total_new)
        new_row_count = carry_count + total_new
        valid_start = self.capacity - new_row_count

        # Reuses nb_segment_extract_fields with the active buffer's own .bundle as the "segment"
        # source - valid because a PooledLogBatch built by this same fetch code has the exact same
        # LogBundle shape (fixed-stride offsets, same has_* flags) a raw ingest segment has, and
        # the kernel never reads segment.size, only per-row offsets/lengths already in the
        # columns/buffer.
        # Carry the decoded-message cache forward alongside the row data, instead of nulling the
        # whole cache every tick: a carried row's bytes are byte-for-byte identical to what
        # nb_segment_extract_fields just copied above, so its already-decoded string is still
        # valid - just living at a new slot in the now-active buffer. Without this, every
        # steady-state row pays a full UTF-8 decode + string allocation again on every tick.
        new_cache = [None] * self.capacity
        if carry_count > 0:
            src_lo = active_valid_start + active_row_count - carry_count
            nb_segment_extract_fields(
                active.bundle,
                self._identity_indices[src_lo : src_lo + carry_count],
                carry_count,
                inactive.bundle,
                valid_start,
                MAX_MSG_BYTES,
            )
            old_cache = self._message_cache
            new_cache[valid_start : valid_start + carry_count] = old_cache[src_lo : src_lo + carry_count]

        self.logger_fetch.debug(
            f"live incr {(time_ns() - t_start) / 1_000_000:.3f} ms | new={total_new} carried={carry_count}"
        )

        self._active = inactive_idx
        self._buf_valid_start[inactive_idx] = valid_start
        self._buf_row_count[inactive_idx] = new_row_count
        self._bind_active()
        self._message_cache = new_cache
        self.last_fetch_new_rows = total_new

    # --- HISTORY mode fetch -------------------------------------------------------------------

    def _fetch_history(self, anchor_seq: Optional[int] = None, anchor_ts: Optional[int] = None):
        boundary = self.HISTORY_BEFORE  # fixed split point between the "before" and "after" regions
        active_bundle = self._batches[self._active].bundle

        # "Before"/"after" write cursors move in opposite directions (right-to-left / left-to-
        # right around the fixed boundary) - same "fill from the right end inward" trick used
        # elsewhere so multi-segment results land in order with no copying.
        write_cursor_before = boundary

        def _consume_before(segment, indices_array, match_count):
            nonlocal write_cursor_before
            write_cursor_before -= match_count
            nb_segment_extract_fields(
                segment.bundle, indices_array, match_count, active_bundle, write_cursor_before, MAX_MSG_BYTES
            )

        write_cursor_after = boundary

        def _consume_after(segment, indices_array, match_count):
            nonlocal write_cursor_after
            nb_segment_extract_fields(
                segment.bundle, indices_array, match_count, active_bundle, write_cursor_after, MAX_MSG_BYTES
            )
            write_cursor_after += match_count

        t_start = time_ns()
        result = self._scanner.scan_history_window(
            anchor_seq=anchor_seq,
            anchor_ts=anchor_ts,
            before_cap=self.HISTORY_BEFORE,
            after_cap=self.HISTORY_AFTER,
            consume_before=_consume_before,
            consume_after=_consume_after,
        )
        before_count = result.before_count
        after_count = result.after_count

        self.logger_fetch.debug(
            f"history {(time_ns() - t_start) / 1_000_000:.3f} ms | before={before_count} after={after_count}"
        )

        self._valid_start = boundary - before_count
        self.row_count = before_count + after_count
        self._before_count = before_count
        self._message_cache = [None] * self.capacity
        self._buf_valid_start[self._active] = self._valid_start
        self._buf_row_count[self._active] = self.row_count


class LogTableCanvas(QAbstractScrollArea):
    """Direct-paint replacement for QTableView + QSortFilterProxyModel + QStyledItemDelegate.

    QAbstractItemView (QTableView's base class) was found to repaint its *entire* viewport on
    any content change - insert, remove, or scroll alike, in both ScrollPerItem and ScrollPerPixel
    modes (confirmed empirically: an instrumented offscreen QTableView logged the full viewport
    rect as dirty for a single-row change) - so per-cell overhead (QModelIndex construction,
    delegate virtual dispatch, per-cell role queries, proxy row-mapping) was pure waste on top of
    a repaint that was always going to cover the whole visible area anyway. This paints the
    visible rows directly from a LogTableStore's arrays in one paintEvent instead, the same way
    Wireshark's packet list avoids QAbstractItemView for the same reason.

    QAbstractScrollArea routes viewport paint/mouse/resize/wheel events to the corresponding
    handler overridden here (paintEvent/mousePressEvent/resizeEvent/wheelEvent/contextMenuEvent),
    exactly as if they'd happened directly on this widget - this is documented QAbstractScrollArea
    behavior (see viewportEvent()), not something being worked around."""

    def __init__(self, store: LogTableStore, parent=None):
        super().__init__(parent)
        self.store = store

        # Single-row click-to-highlight selection, keyed by backend sequence id (not row index)
        # so it survives a live tick shifting row indices via eviction - mirrors the persistent-
        # index behavior QTableView gave for free.
        self.selected_seq = None

        # Callback the owning widget hooks to react to "scrolled up while live" (enters history
        # mode) - kept as a plain attribute rather than a Qt signal since there's exactly one
        # subscriber and it's always set before first use.
        self.on_wheel_up_while_live = None

        self._col_order = list(LogTableCol)
        self._col_visible = {col: True for col in LogTableCol}
        self._col_visible[LogTableCol.RX_TIMESTAMP] = False
        self._col_visible[LogTableCol.PROCESS] = False
        self._col_visible[LogTableCol.THREAD] = False
        self._col_width = {col: 80 for col in LogTableCol}

        self._col_autosized_once: set = set()
        self._col_autosize_last_ns: dict = {}

        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)  # LIVE mode default - no scrollback to reach
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)  # MESSAGE stretches to fill remaining width
        self.setFrameShape(QAbstractScrollArea.NoFrame)
        self.verticalScrollBar().valueChanged.connect(lambda _v: self.viewport().update())

        self._recompute_message_column_width()

    # --- Layout ------------------------------------------------------------------------------

    def visible_row_count(self) -> int:
        h = self.viewport().height() - HEADER_HEIGHT
        return max(0, h // ROW_HEIGHT)

    def first_visible_row(self) -> int:
        if self.store.mode == LogViewMode.LIVE:
            return 0
        return self.verticalScrollBar().value()

    def _visible_columns(self):
        return [c for c in self._col_order if self._col_visible[c]]

    def _column_x_offsets(self) -> dict:
        offsets = {}
        x = 0
        for col in self._visible_columns():
            offsets[col] = x
            x += self._col_width[col]
        return offsets

    def _recompute_message_column_width(self):
        visible = self._visible_columns()
        if LogTableCol.MESSAGE not in visible:
            return
        others_width = sum(self._col_width[c] for c in visible if c != LogTableCol.MESSAGE)
        remaining = self.viewport().width() - others_width
        self._col_width[LogTableCol.MESSAGE] = max(80, remaining)

    def _update_scrollbar_range(self):
        visible = self.visible_row_count()
        maximum = max(0, self.store.row_count - visible)
        bar = self.verticalScrollBar()
        bar.setRange(0, maximum)
        bar.setPageStep(max(1, visible))
        bar.setSingleStep(1)

    def sync_viewport_rows(self):
        self.store.set_viewport_rows(max(1, self.visible_row_count()))

    def request_repaint(self):
        """Call after any LogTableStore call that may have changed row_count/content - there are
        no Qt model signals to do this automatically anymore."""
        self._update_scrollbar_range()
        self._recompute_message_column_width()
        self.viewport().update()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.sync_viewport_rows()
        self.request_repaint()

    # --- Painting ------------------------------------------------------------------------------

    def paintEvent(self, event):
        painter = QPainter(self.viewport())
        try:
            painter.fillRect(self.viewport().rect(), self.palette().base())
            self._paint_header(painter)
            self._paint_rows(painter)
        finally:
            painter.end()

    def _paint_header(self, painter: QPainter):
        painter.save()
        header_rect = QRect(0, 0, self.viewport().width(), HEADER_HEIGHT)
        painter.fillRect(header_rect, self.palette().button())
        painter.setPen(self.palette().buttonText().color())
        offsets = self._column_x_offsets()
        for col in self._visible_columns():
            rect = QRect(offsets[col], 0, self._col_width[col], HEADER_HEIGHT).adjusted(4, 0, -4, 0)
            painter.drawText(rect, Qt.AlignVCenter | Qt.AlignLeft, _COLUMN_LABELS[col])
        painter.restore()

    def _paint_rows(self, painter: QPainter):
        store = self.store
        first = self.first_visible_row()
        last = min(first + self.visible_row_count(), store.row_count)
        if last <= first:
            return

        offsets = self._column_x_offsets()
        visible_columns = self._visible_columns()
        viewport_width = self.viewport().width()
        selected_seq = self.selected_seq
        default_color = self.palette().text().color()
        highlight_brush = self.palette().highlight()
        highlight_text_color = self.palette().highlightedText().color()

        painter.save()
        for offset, row in enumerate(range(first, last)):
            y = HEADER_HEIGHT + offset * ROW_HEIGHT
            is_selected = selected_seq is not None and store.seq_for_row(row) == selected_seq

            if is_selected:
                painter.fillRect(QRect(0, y, viewport_width, ROW_HEIGHT), highlight_brush)
                painter.setPen(highlight_text_color)
            else:
                color = store.get_color(row)
                painter.setPen(color if color is not None else default_color)

            for col in visible_columns:
                text = store.get_cell(row, col)
                rect = QRect(offsets[col], y, self._col_width[col], ROW_HEIGHT).adjusted(4, 0, -4, 0)
                painter.drawText(rect, Qt.AlignVCenter | Qt.AlignLeft, "" if text is None else text)
        painter.restore()

    # --- Selection / interaction -----------------------------------------------------------

    def _row_at(self, pos) -> Optional[int]:
        if pos.y() < HEADER_HEIGHT:
            return None
        first = self.first_visible_row()
        row = first + (pos.y() - HEADER_HEIGHT) // ROW_HEIGHT
        if first <= row < min(first + self.visible_row_count(), self.store.row_count):
            return row
        return None

    def mousePressEvent(self, event):
        row = self._row_at(event.pos())
        self.selected_seq = self.store.seq_for_row(row) if row is not None else None
        self.viewport().update()
        super().mousePressEvent(event)

    def contextMenuEvent(self, event):
        row = self._row_at(event.pos())
        if row is None:
            return

        menu = QMenu(self)
        action_copy = QAction("Copy Message", self)
        action_copy.triggered.connect(lambda: self._copy_message(row))
        menu.addAction(action_copy)
        menu.exec_(event.globalPos())

    def _copy_message(self, row: int):
        QApplication.clipboard().setText(self.store.get_cell(row, LogTableCol.MESSAGE) or "")

    def wheelEvent(self, event):
        if self.store.mode == LogViewMode.LIVE and event.angleDelta().y() > 0:
            # Scrolling "up" (towards older entries) while live: hand off to the widget to anchor
            # on whatever's currently on top and switch into a scrollable history window.
            if self.on_wheel_up_while_live is not None:
                self.on_wheel_up_while_live()
            event.accept()
            return
        super().wheelEvent(event)

    # --- Column visibility / auto-sizing -----------------------------------------------------

    def set_column_visible(self, col: LogTableCol, visible: bool):
        self._col_visible[col] = visible
        if visible:
            self.reset_column_autosize(col)
        self._recompute_message_column_width()
        self.viewport().update()

    def reset_column_autosize(self, col: LogTableCol):
        self._col_autosized_once.discard(col)
        self._col_autosize_last_ns.pop(col, None)

    def autosize_columns(self):
        """Sizes columns to fit their content. TIMESTAMP/RX_TIMESTAMP/LEVEL/THREAD are sized
        once (their content width is effectively fixed for a given ts_precision - see
        reset_column_autosize for why TIMESTAMP/RX_TIMESTAMP get an extra explicit re-trigger on
        precision change) and then left alone; MODULE/PROCESS are re-measured periodically
        (throttled - live mode ticks at ~10Hz) since new, differently-sized module/process names
        keep appearing as rows stream in. MESSAGE is intentionally excluded - it stretches to
        fill the remaining space and DEVICE rarely varies enough to matter."""
        store = self.store
        if store.row_count == 0:
            return

        metrics = QFontMetrics(self.font())
        changed = False

        once_cols = (LogTableCol.TIMESTAMP, LogTableCol.RX_TIMESTAMP, LogTableCol.LEVEL, LogTableCol.THREAD)
        for col in once_cols:
            if self._col_visible[col] and col not in self._col_autosized_once:
                self._measure_column(col, metrics)
                self._col_autosized_once.add(col)
                changed = True

        now = time_ns()
        throttled_cols = (LogTableCol.MODULE, LogTableCol.PROCESS)
        for col in throttled_cols:
            if not self._col_visible[col]:
                continue
            last = self._col_autosize_last_ns.get(col, 0)
            if now - last >= COLUMN_AUTOSIZE_THROTTLE_NS:
                self._measure_column(col, metrics)
                self._col_autosize_last_ns[col] = now
                changed = True

        if changed:
            self._recompute_message_column_width()
            self.viewport().update()

    def _measure_column(self, col: LogTableCol, metrics: QFontMetrics):
        store = self.store
        first = self.first_visible_row()
        last = min(first + self.visible_row_count(), store.row_count)
        width = metrics.horizontalAdvance(_COLUMN_LABELS[col])
        for row in range(first, last):
            text = store.get_cell(row, col)
            if text:
                width = max(width, metrics.horizontalAdvance(text))
        self._col_width[col] = width + 8


@register_widget_factory(WidgetName.LOG_TABLE_VIEWER)
class LogTableViewerWidget(QWidget):
    """Table-based alternative to LogViewerWidget, sharing the same LogFilter/ModuleFilterSidebar
    filtering capability but rendering structured rows directly via LogTableCanvas instead of
    formatted text.

    Runs in two tiers (see LogTableStore): a scrollbar-less LIVE tail view that only fetches
    enough rows to fill the visible area, and a bounded HISTORY view (entered on scroll) that
    fetches a couple hundred rows before/after the point the user scrolled away from."""

    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)

        self.gui_context: GUIContext = gui_context

        # Same Pause button highlight scheme as LogViewerWidget, so both viewers present
        # auto vs. manual pausing identically.
        self.setStyleSheet("""QToolButton {
    border-radius: 4px;
    padding: 2px;
}

/* Auto-Pause Highlight */
QToolButton[autoPaused="true"] {
    background-color: #882222; /* Deep Red */
    color: white;
    border: 1px solid #ff4444;
}

/* Optional: Manual Pause Highlight (Amber) */
QToolButton[manualPaused="true"] {
    background-color: #886622;
    color: white;
}
""")

        self.tab_name = ""
        self.allowed_device = None
        self.filtered_module = None
        self.filtered_module_children = False
        self.log_level = LogLevel.ALL.name_conf
        self.filter_sidebar_state = None
        self.show_module_filter = False
        self.show_hidden = False
        self.show_rx_ts = False
        self.show_process_thread = False
        self.ts_precision = 3
        self.kv_filter_text = ""
        self.search_text = ""

        # Playback-follow state machine (see plans/playback-follow-state-machine.md) - replaces
        # what used to be four loose booleans (follow_playback/is_paused/_playback_anchored/
        # force_live) with one FollowState enum + explicit transition table. Constructed before
        # _set_defaults()/restore() below since restore() seeds force_live through it.
        self._playback = PlaybackFollowMachine(supports_freeze=True)

        self._set_defaults()

        if state:
            self.restore(state)

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        self.toolbar = QToolBar("Log Table Viewer Toolbar", self)
        self.toolbar.setMovable(False)
        self.layout.addWidget(self.toolbar)

        self.action_toggle_filter = QAction("⧨ Filter", self)
        self.action_toggle_filter.setCheckable(True)
        self.action_toggle_filter.setChecked(self.show_module_filter)
        self.action_toggle_filter.setToolTip("Toggle Module Filter Sidebar")
        self.action_toggle_filter.toggled.connect(self._toggle_module_filter)
        self.toolbar.addAction(self.action_toggle_filter)

        self.toolbar.addSeparator()

        self.level_combo = QComboBox()
        for lvl in LogLevel.LIST_UI:
            self.level_combo.addItem(lvl.name_conf, lvl)
        self.toolbar.addWidget(self.level_combo)
        self.level_combo.currentIndexChanged.connect(self._handle_level_change)

        self.toolbar.addSeparator()

        self.time_options_btn = QToolButton()
        self.time_options_btn.setText("Time ▾")
        self.time_options_btn.setPopupMode(QToolButton.InstantPopup)
        self.time_options_menu = QMenu(self)

        self.action_show_rx_ts = QAction("Show Receive Time", self)
        self.action_show_rx_ts.setCheckable(True)
        self.action_show_rx_ts.setChecked(self.show_rx_ts)
        self.action_show_rx_ts.toggled.connect(self._toggle_rx_ts)
        self.time_options_menu.addAction(self.action_show_rx_ts)

        self.time_options_menu.addSeparator()

        self.precision_group = QActionGroup(self)
        self.precision_group.setExclusive(True)

        precisions = [("Seconds (s)", 0), ("Milliseconds (ms)", 3), ("Microseconds (us)", 6), ("Nanoseconds (ns)", 9)]
        for label, prec_val in precisions:
            act = QAction(label, self)
            act.setCheckable(True)
            act.setChecked(self.ts_precision == prec_val)
            act.triggered.connect(lambda checked, p=prec_val: self._set_ts_precision(p))
            self.precision_group.addAction(act)
            self.time_options_menu.addAction(act)

        self.time_options_btn.setMenu(self.time_options_menu)
        self.toolbar.addWidget(self.time_options_btn)

        self.toolbar.addSeparator()

        self.action_show_process_thread = QAction("Process/Thread", self)
        self.action_show_process_thread.setCheckable(True)
        self.action_show_process_thread.setChecked(self.show_process_thread)
        self.action_show_process_thread.setToolTip(
            "Show Process/Thread columns (resolved from PID history - populated by ADB sources only)"
        )
        self.action_show_process_thread.toggled.connect(self._toggle_process_thread)
        self.toolbar.addAction(self.action_show_process_thread)

        self.toolbar.addSeparator()

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Filter device/module/message...")
        self.search_box.setClearButtonEnabled(True)
        self.search_box.setText(self.search_text)
        self.toolbar.addWidget(self.search_box)

        # Debounced and baked into the same row-level Numba filter kernels as the kv/level
        # filters (LogFilter.set_text_filter/bake_text_search), rather than a Qt proxy over
        # whatever rows happened to already be fetched - so a live-mode fetch actually re-scans
        # the backend and fills the viewport with matches.
        self._search_timer = QTimer(self)
        self._search_timer.setSingleShot(True)
        self._search_timer.setInterval(200)
        self.search_box.textChanged.connect(lambda _text: self._search_timer.start())

        self.toolbar.addSeparator()

        self.kv_filter_box = KvFilterLineEdit()
        self.kv_filter_box.setMaximumWidth(240)
        self.kv_filter_box.setText(self.kv_filter_text)
        self.toolbar.addWidget(self.kv_filter_box)

        self.toolbar.addSeparator()

        self.action_clear = QAction("Clear", self)
        self.action_clear.triggered.connect(self.clear_logs)
        self.toolbar.addAction(self.action_clear)

        self.auto_paused = False
        self._is_catching_up = True

        # Last clock.current_ts_ns this tab actually re-fetched under while following - lets
        # apply_updates() skip a redundant kernel scan every ~100ms when REPLAY is paused and
        # sitting still.
        self._last_followed_ts_ns = None
        # Dedicated throttle for the follow branch, separate from model.prev_apply (LIVE-mode
        # fetch throttling).
        self.prev_apply = 0

        # Velocity Tracking - same clog-protection mechanism as LogViewerWidget
        self.velocity_tracker = LogVelocityTracker(limit_per_sec=1000)

        # Pause/Resume toggle - same texts/behavior as LogViewerWidget.action_pause. Pause and
        # history mode are the same state from the user's perspective (see _set_pause_ui), so
        # this single button replaces the old non-checkable "Go Live" action.
        self.action_pause = QAction("⏸ Pause", self)
        self.action_pause.setCheckable(True)
        self.action_pause.toggled.connect(self._toggle_pause)
        self.toolbar.insertAction(self.action_clear, self.action_pause)

        # "Live" override - mirrors LogViewerWidget.action_force_live: pins this tab to the live
        # tail while the global transport is scrubbing REPLAY. Only shown while REPLAY is
        # actually active (see apply_updates/_sync_force_live_visibility).
        self.action_force_live = QAction("Live", self)
        self.action_force_live.setCheckable(True)
        self.action_force_live.setChecked(self.force_live)
        self.action_force_live.setToolTip("Keep this tab on live data while the global transport is scrubbing REPLAY")
        self.action_force_live.toggled.connect(self._toggle_force_live)
        self.toolbar.insertAction(self.action_clear, self.action_force_live)
        self._sync_force_live_visibility(self._clock())

        self.splitter = QSplitter(Qt.Horizontal, self)
        self.layout.addWidget(self.splitter)

        self.log_filter = LogFilter(
            self.gui_context.id_registry,
            self.allowed_device,
            self.filtered_module,
            log_level=self.log_level,
            filtered_module_children=self.filtered_module_children,
        )
        self.log_filter.set_kv_filter(self.kv_filter_text)
        self.kv_filter_box.filterTextCommitted.connect(self._apply_kv_filter_text)

        self.log_filter.set_text_filter(self.search_text)
        self._search_timer.timeout.connect(self._apply_search_text)

        self.filter_sidebar = ModuleFilterSidebar(
            gui_context=self.gui_context, target_filter=self.log_filter, parent=self, show_hidden=self.show_hidden
        )
        self.filter_sidebar.restore_state(self.filter_sidebar_state)
        self.filter_sidebar.log_filter.filter_changed.connect(self._reload_and_redraw)
        self.filter_sidebar.action_enable.toggled.connect(lambda _checked: self._reload_and_redraw())
        self.filter_sidebar.setMinimumWidth(200)
        self.splitter.addWidget(self.filter_sidebar)
        self.filter_sidebar.setVisible(self.show_module_filter)

        self.model = LogTableStore(self.gui_context, self.log_filter, self.filter_sidebar)

        self.view = LogTableCanvas(self.model)
        self.view.setMinimumWidth(300)
        self.view.on_wheel_up_while_live = self._on_scrolled_away_from_live
        self.view.set_column_visible(LogTableCol.RX_TIMESTAMP, self.show_rx_ts)
        self.view.set_column_visible(LogTableCol.PROCESS, self.show_process_thread)
        self.view.set_column_visible(LogTableCol.THREAD, self.show_process_thread)

        self._programmatic_scroll = False
        self.prev_history_poll = 0
        self._last_polled_backend_seq = None
        self.view.verticalScrollBar().valueChanged.connect(self._on_scroll_value_changed)

        self.splitter.addWidget(self.view)
        self.splitter.setStretchFactor(0, 2)
        self.splitter.setStretchFactor(1, 8)

        show_filter_btn = self.filtered_module is None or self.filtered_module_children
        self.action_toggle_filter.setVisible(show_filter_btn)

        idx = self.level_combo.findData(LogLevel.from_string(self.log_level))
        if idx != -1:
            self.level_combo.setCurrentIndex(idx)

        self.view.sync_viewport_rows()
        self.view.request_repaint()

        self.gui_context.add_updatable(self)

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__
        self.allowed_device = None
        self.filtered_module = None
        self.filtered_module_children = False
        self.log_level = None

    def restore(self, state: dict):
        self.tab_name = state.get("tab_name", self.tab_name)

        self.show_hidden = state.get("show_hidden", self.show_hidden)

        self.allowed_device = self.gui_context.id_registry.resolve_device(
            state.get("allowed_device", self.allowed_device)
        )
        self.filtered_module = self.gui_context.id_registry.resolve_module(
            state.get("filtered_module", self.filtered_module)
        )
        self.filtered_module_children = state.get("filtered_module_children", self.filtered_module_children)
        self.log_level = state.get("log_level", self.log_level)

        view_state = state.get("view_state", {})
        self.show_module_filter = view_state.get("show_module_filter", self.show_module_filter)
        # Checked in both places: get_state()'s own round-trip nests it under view_state, but a
        # freshly-opened tab spawned from TelemetryTable (see _trigger_module_action) passes it
        # as a top-level param instead - top-level wins so a fresh spawn isn't shadowed by a
        # (nonexistent) view_state default.
        self.force_live = state.get("force_live", view_state.get("force_live", self.force_live))
        self.show_rx_ts = view_state.get("show_rx_ts", self.show_rx_ts)
        self.show_process_thread = view_state.get("show_process_thread", self.show_process_thread)
        self.ts_precision = view_state.get("ts_precision", self.ts_precision)
        self.kv_filter_text = view_state.get("kv_filter_text", self.kv_filter_text)
        self.search_text = view_state.get("search_text", self.search_text)
        self.filter_sidebar_state = state.get("filter_sidebar", self.filter_sidebar_state)

    def get_state(self):
        return {
            "tab_name": self.tab_name,
            "allowed_device": self.allowed_device.name if self.allowed_device else None,
            "filtered_module": f"{self.filtered_module.name_with_device()}" if self.filtered_module else None,
            "filtered_module_children": self.filtered_module_children,
            "view_state": {
                "show_module_filter": self.show_module_filter,
                "force_live": self.force_live,
                "show_rx_ts": self.show_rx_ts,
                "show_process_thread": self.show_process_thread,
                "ts_precision": self.ts_precision,
                "kv_filter_text": self.log_filter.kv_filter_text,
                "search_text": self.log_filter.text_filter_text,
            },
            "log_level": self.log_filter.log_level.name_conf,
            "filter_sidebar": self.filter_sidebar.get_state(),
            "show_hidden": self.filter_sidebar.action_show_non_essential.isChecked(),
        }

    def _clock(self):
        """Read-only access to the global registry.playback_clock. Only PlaybackControlWidget
        ever calls clock.tick() (it's constructed before any tab, so it always ticks first in
        the same GUIContext heartbeat) - this tab must only read mode/current_ts_ns/is_playing,
        never advance it itself."""
        registry = self.gui_context.registry
        return registry.playback_clock if registry is not None else None

    def _clock_snapshot(self, clock) -> ClockSnapshot:
        if clock is None:
            return ClockSnapshot(mode=PlaybackMode.LIVE, current_ts_ns=0)
        return ClockSnapshot(mode=clock.mode, current_ts_ns=clock.current_ts_ns, is_playing=clock.is_playing)

    @property
    def is_paused(self) -> bool:
        return self._playback.state is FollowState.FROZEN

    @property
    def follow_playback(self) -> bool:
        return self._playback.state is not FollowState.FROZEN

    @property
    def _playback_anchored(self) -> bool:
        # A fetch that found nothing at this instant leaves mode/row_count showing whatever was
        # there before - only claim "clock-anchored" once a real ts-anchored window with actual
        # rows is on screen, same invariant the old raw _playback_anchored flag preserved by only
        # ever being set True after a successful fetch.
        return (
            self._playback.state is FollowState.FOLLOWING
            and self.model.mode == LogViewMode.HISTORY
            and self.model.row_count > 0
        )

    @property
    def force_live(self) -> bool:
        return self._playback.force_live

    @force_live.setter
    def force_live(self, value: bool):
        self._playback.force_live = value

    def _apply_freeze(self, action):
        """Executes a FollowActionKind.FREEZE action - mirrors LogViewerWidget._apply_freeze.
        from_state LIVE -> anchor off the top-of-viewport live row (_enter_history_at_top_row);
        FOLLOWING -> the ts-anchored window already on screen is kept in place, just frozen."""
        if action.from_state is FollowState.LIVE:
            self._enter_history_at_top_row(auto=action.auto)
            if self.model.mode != LogViewMode.HISTORY:
                # No live rows yet to build a window from - still honor the freeze request.
                self._set_pause_ui(True, auto=action.auto)
        elif action.from_state is FollowState.FOLLOWING:
            self._set_pause_ui(True, auto=action.auto)

    def _on_scrolled_away_from_live(self):
        """Wired to LogTableCanvas.on_wheel_up_while_live - LIVE mode has ScrollBarAlwaysOff, so
        there's no scrollbar valueChanged to detect a manual scroll-away the way
        _on_scroll_value_changed does for HISTORY; the canvas's own wheel handling calls this
        directly instead."""
        action = self._playback.handle(FollowEvent.ScrolledAway(), self._clock_snapshot(self._clock()))
        self._apply_freeze(action)

    def apply_updates(self):
        now_ns = self.gui_context.registry.now_ns
        t_start = now_ns()

        clock = self._clock()

        self._sync_force_live_visibility(clock)

        # Drive the shared playback-follow state machine (see
        # plans/playback-follow-state-machine.md) with this heartbeat's clock snapshot.
        action = self._playback.handle(FollowEvent.Tick(), self._clock_snapshot(clock))

        if action.kind is FollowActionKind.FETCH_LIVE and action.from_state is not FollowState.LIVE:
            # Edge-triggered transition into LIVE (REPLAY -> LIVE, or force_live catching a
            # FOLLOWING tab): resume the ordinary live tail the same way unpausing does.
            self._last_followed_ts_ns = None
            self._go_live()
            return

        if action.kind is FollowActionKind.FETCH_FOLLOWING:
            # Re-anchor to the clock's virtual time whenever it's playing back, throttled on the
            # same cadence as the live tail fetch.
            if t_start - self.prev_apply < 100_000_000:
                return
            self.prev_apply = t_start
            # Skip the refetch once REPLAY is paused and the clock hasn't moved since the last
            # follow - avoids a full kernel scan every ~100ms per open tab for no visual change.
            if clock.is_playing or clock.current_ts_ns != self._last_followed_ts_ns:
                self._reanchor_history(anchor_ts=action.anchor_ts_ns)
                self._last_followed_ts_ns = clock.current_ts_ns
            return

        self.model.apply_updates()

        if self.model.mode == LogViewMode.LIVE:
            # Clog protection, mirroring LogViewerWidget.apply_updates: a live tail that's
            # matching more rows than fit the viewport every tick is silently dropping the
            # backlog beyond viewport_rows each fetch - freeze into a browsable history window
            # instead so nothing is lost unnoticed.
            was_catching_up = self._is_catching_up
            reached_live_edge = self.model.last_fetch_new_rows < self.model.viewport_rows
            if self._is_catching_up and reached_live_edge:
                self._is_catching_up = False

            if self.model.last_fetch_new_rows > 0:
                if was_catching_up:
                    # The initial burst of backlog while first catching up to live doesn't count
                    # as a sustained clog - same carve-out as LogViewerWidget.
                    self.velocity_tracker.reset()
                elif self.velocity_tracker.update_and_check(self.model.last_fetch_new_rows):
                    clog_action = self._playback.handle(FollowEvent.ClogDetected(), self._clock_snapshot(clock))
                    self._apply_freeze(clog_action)
                    return

        if self.model.last_fetch_changed:
            self.view.request_repaint()
            self.view.autosize_columns()

        if self.model.mode == LogViewMode.HISTORY:
            self._poll_history_tail()

    def _poll_history_tail(self):
        """Mirrors LogViewerWidget._poll_history_tail (qt-log-table-viewer skill Sec 9): a sparse
        filter's history window can fit entirely inside the viewport, collapsing the scrollbar's
        range to "nothing to scroll" (min == max) - valueChanged then never fires again, so
        _on_scroll_value_changed's bottom-edge catch-up check never runs. Piggyback on the
        heartbeat instead so a stalled-at-the-bottom history view still notices new rows."""
        now = time_ns()
        if now - self.prev_history_poll < 100_000_000:
            return
        self.prev_history_poll = now

        if self.model.anchor_seq is None:
            return

        scrollbar = self.view.verticalScrollBar()
        if scrollbar.value() < scrollbar.maximum() - 1:
            return  # Not at the bottom - the scroll handler already covers catch-up once they get there

        pool = self.gui_context.registry.central.log_pool
        latest = pool.latest_sequence()
        if latest == self._last_polled_backend_seq:
            return  # Backend hasn't advanced since we last looked - nothing new to find
        self._last_polled_backend_seq = latest

        if self._at_live_edge():
            self._go_live()
            return

        # Deliberately not gated on `last_seq != self.model.anchor_seq` - see the matching
        # comment in _on_scroll_value_changed's bottom-edge branch. Also not gated on
        # row_count > 0 - see _history_newest_ref_seq's anchor_seq fallback.
        last_seq = self._history_newest_ref_seq()
        if last_seq is not None:
            self._reanchor_history(last_seq)

    def _handle_level_change(self, index):
        level_identity = self.level_combo.itemData(index)
        self.log_filter.set_level(level_identity.name_conf)
        self._reload_and_redraw()

    def _reload_and_redraw(self):
        self.model.reload_and_redraw()
        self.view.request_repaint()
        self.view.autosize_columns()

    def _apply_kv_filter_text(self, text):
        self.log_filter.set_kv_filter(text)
        self._reload_and_redraw()

    def _apply_search_text(self):
        self.log_filter.set_text_filter(self.search_box.text())
        self._reload_and_redraw()

    def _toggle_module_filter(self, checked):
        self.show_module_filter = checked
        self.filter_sidebar.setVisible(checked)

    def _toggle_rx_ts(self, checked):
        self.show_rx_ts = checked
        self.view.set_column_visible(LogTableCol.RX_TIMESTAMP, checked)
        if checked:
            self.view.autosize_columns()

    def _toggle_process_thread(self, checked):
        self.show_process_thread = checked
        self.view.set_column_visible(LogTableCol.PROCESS, checked)
        self.view.set_column_visible(LogTableCol.THREAD, checked)
        if checked:
            self.view.autosize_columns()

    def _set_ts_precision(self, precision: int):
        if self.ts_precision != precision:
            self.ts_precision = precision
            self.model.set_ts_precision(precision)
            # A precision change alone doesn't touch row_count, so request_repaint()'s scrollbar-
            # range recompute is a no-op here - still needed for the viewport().update() call.
            self.view.reset_column_autosize(LogTableCol.TIMESTAMP)
            self.view.reset_column_autosize(LogTableCol.RX_TIMESTAMP)
            self.view.autosize_columns()
            self.view.request_repaint()

    def clear_logs(self):
        self.model.clear_logs()
        self.view.selected_seq = None
        self._set_live_ui_state()
        self.view.request_repaint()

    # --- Live/history mode transitions -----------------------------------------------------

    def _topmost_row_seq(self):
        return self.model.seq_for_row(self.view.first_visible_row())

    def _enter_history_at_top_row(self, auto: bool = False):
        """Called the moment the user scrolls away from the live tail, or when clog protection
        (auto=True) needs to freeze the view. Anchors the new history window on whichever live
        row is currently at the top of the viewport, so the view doesn't visually jump."""
        anchor_seq = self._topmost_row_seq()
        if anchor_seq is None:
            return
        self._reanchor_history(anchor_seq, auto=auto)

    def _reanchor_history(self, anchor_seq=None, anchor_ts=None, auto: bool = False):
        """Rebuilds the history window around anchor_seq or anchor_ts (mutually exclusive) and
        repositions the view on it. Both enter_history_mode() and the scrollbar reposition below
        change the scrollbar's value programmatically, which would otherwise re-fire valueChanged
        and recurse straight back into _on_scroll_value_changed - the guard flag suppresses that
        re-entrancy."""
        was_live = self.model.mode != LogViewMode.HISTORY
        self._programmatic_scroll = True
        try:
            self.model.enter_history_mode(anchor_seq=anchor_seq, anchor_ts=anchor_ts)
            self.view.request_repaint()  # scrollbar range must reflect the new row_count first

            if was_live:
                # The scrollbar must become interactive on the live->history transition edge
                # regardless of anchor kind - ScrollBarAlwaysOff (LIVE mode's default) would
                # otherwise make it impossible for the user to ever scroll away from a
                # ts-anchored follow view, since LogTableCanvas.wheelEvent's LIVE-only "scroll
                # up" hook never fires once already in HISTORY mode.
                self._set_history_ui_state()
                # Pause and history mode are the same state (see _set_pause_ui) - but skipped for
                # a ts-anchored (playback-following) transition: is_paused must stay False there,
                # or the very next apply_updates() tick's `not self.is_paused` follow-guard would
                # immediately block further following - freezing the tab after just one reanchor
                # until a manual Resume click, which defeats the point of following the clock at
                # all. Also only synced on this live->history edge, not on every page-through
                # reanchor while already browsing history.
                if anchor_ts is None:
                    self._set_pause_ui(True, auto=auto)
            else:
                # If this fetch's "after" set already reaches the backend's latest row, there may
                # be too little remaining content to fill the viewport - the scrollbar's range can
                # collapse to "nothing to scroll", meaning valueChanged will never fire again to
                # tell us we've caught up. Check eagerly here instead of only reacting to future
                # scrolls (qt-log-table-viewer skill Sec 9).
                #
                # Only applies when paging forward within an already-open SEQ-anchored history
                # window (was_live is False, anchor_ts is None) - on the initial live->history
                # transition the anchor is deliberately right at/near the tail, so this would
                # otherwise immediately bounce straight back to live and undo the pause the user
                # just asked for. A ts-anchored (playback-following) window's LIVE/REPLAY
                # transitions are solely driven by the global clock (see apply_updates), never by
                # this heuristic, or it would spuriously snap back to live-tail mid-follow.
                if anchor_ts is None and self._at_live_edge():
                    self._go_live()
                    return

            self._scroll_to_row(self.model.anchor_scroll_row())
            self.view.autosize_columns()
        finally:
            self._programmatic_scroll = False

    def _history_newest_ref_seq(self):
        """The reference point for "how far forward has this history window scanned", mirroring
        LogViewerWidget.history_newest_seq: the last actually-matched row when there is one,
        otherwise the anchor itself. _fetch_history's "after" scan runs from the anchor forward
        through every remaining segment regardless of whether anything matches, so anchor_seq is
        still a valid low-water mark even when a sparse filter leaves row_count at 0 - without
        this fallback, a window with zero matches could never be recognized as caught up to the
        live edge and would poll forever without ever resuming live mode."""
        if self.model.row_count > 0:
            return self.model.seq_for_row(self.model.row_count - 1)
        return self.model.anchor_seq

    def _at_live_edge(self) -> bool:
        last_seq = self._history_newest_ref_seq()
        if last_seq is None:
            return False
        pool = self.gui_context.registry.central.log_pool
        return last_seq >= pool.latest_sequence()

    def _scroll_to_row(self, row):
        if row is None or row < 0:
            return
        self.view.verticalScrollBar().setValue(row)

    def _go_live(self):
        """Also the mechanism used to explicitly return to live mode (unpausing, or catching up
        while paging history forward) - mirrors LogViewerWidget._redraw_history."""
        self._programmatic_scroll = True
        try:
            self.model.enter_live_mode()
        finally:
            self._programmatic_scroll = False
        self.view.request_repaint()
        self.view.autosize_columns()
        self._set_live_ui_state()
        self._set_pause_ui(False)
        self._is_catching_up = True
        # This is the single authoritative "make it live" mechanic - force the machine to LIVE
        # directly (not via .handle(), there's no dedicated event for "someone decided we're live
        # now") rather than trusting every caller to have already transitioned it - mirrors
        # LogViewerWidget._redraw_history. Covers _reanchor_history's/_poll_history_tail's own
        # internal "caught up while paging forward" _at_live_edge() fallbacks too, which never
        # went through a transition of their own.
        self._playback.state = FollowState.LIVE

    def _set_live_ui_state(self):
        self.view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

    def _set_history_ui_state(self):
        self.view.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)

    def _toggle_pause(self, checked):
        """Pause and history mode are the same state from the user's perspective: pausing means
        freezing on a browsable window, and scrolling away from the tail already does that.
        Fires from the user clicking the Pause button (checked=Qt's new state). Routes entirely
        through the shared playback-follow machine - mirrors LogViewerWidget._toggle_pause."""
        clock = self._clock()
        action = self._playback.handle(FollowEvent.TogglePause(checked), self._clock_snapshot(clock))

        if action.kind is FollowActionKind.FREEZE:
            self._apply_freeze(action)
        elif action.kind is FollowActionKind.FETCH_LIVE:
            self._go_live()
        elif not checked and self._playback.state is FollowState.FOLLOWING:
            # Rejoining REPLAY-follow: no immediate fetch (the next apply_updates() tick's
            # FOLLOWING branch re-anchors to wherever the clock has moved to since) - just clear
            # the freeze UI.
            self._set_pause_ui(False)

    def _toggle_force_live(self, checked: bool):
        """Pins this tab to the live tail regardless of the global clock's REPLAY scrubbing.
        Mirrors LogViewerWidget._toggle_force_live - is_paused always wins (a manually-paused
        tab only jumps to live once the user clicks Resume, see _toggle_pause) - the machine's
        own ToggleForceLive handling already encodes that (a no-op while FROZEN)."""
        clock = self._clock()
        action = self._playback.handle(FollowEvent.ToggleForceLive(checked), self._clock_snapshot(clock))
        if action.kind is FollowActionKind.FETCH_LIVE:
            self._go_live()

    def _sync_force_live_visibility(self, clock):
        """The Live override only means something while the global transport is actually
        scrubbing REPLAY - hidden the rest of the time. force_live itself is untouched by this,
        so re-entering REPLAY later remembers whatever this tab had it set to."""
        self.action_force_live.setVisible(clock is not None and clock.mode is PlaybackMode.REPLAY)

    def _set_pause_ui(self, paused: bool, auto: bool = False):
        """Syncs self.is_paused/self.auto_paused and the Pause button's text/checked/style to
        match. Mirrors LogViewerWidget._set_pause_ui exactly (including the button text scheme)
        so both viewers present pausing identically."""
        # self.is_paused is a read-only property derived from self._playback.state - this only
        # syncs the button's own text/checked/style, never the underlying state itself.
        self.auto_paused = paused and auto

        self.action_pause.blockSignals(True)
        self.action_pause.setChecked(paused)
        self.action_pause.blockSignals(False)

        self.action_pause.setText(("▶ Resume (AUTO)" if self.auto_paused else "▶ Resume") if paused else "⏸ Pause")

        if not paused:
            self.velocity_tracker.reset()

        button = self.toolbar.widgetForAction(self.action_pause)
        if button:
            button.setProperty("autoPaused", self.auto_paused)
            button.setProperty("manualPaused", paused and not self.auto_paused)
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

    def _on_scroll_value_changed(self, value):
        if self._programmatic_scroll:
            return

        if self.model.mode != LogViewMode.HISTORY:
            return

        if self._playback_anchored:
            clock = self._clock()
            if clock is not None and clock.is_scrubbing:
                # A drag on the global transport scrubber re-anchors this tab's history window
                # on every follow tick (see apply_updates), and that reanchor's own
                # request_repaint()/scrollbar-range recompute can trigger a scrollbar
                # valueChanged here that Qt defers past the `_programmatic_scroll` guard window
                # closing (its geometry/range recompute isn't always fully synchronous). While a
                # scrub is actively in progress, this can never be a genuine manual scroll of the
                # table's own viewport, so ignore it instead of wrongly detaching/pausing.
                return

            # A manual scroll within a playback-following window locally overrides it for this
            # tab only - the global clock and every other tab keep going. The machine resets back
            # to FOLLOWING either when the clock itself goes back to LIVE (apply_updates'
            # FETCH_LIVE edge) or when the user explicitly clicks Resume while the clock is still
            # REPLAY (_toggle_pause) - not merely by scrolling back to the pool's current edge.
            action = self._playback.handle(FollowEvent.ScrolledAway(), self._clock_snapshot(clock))
            self._apply_freeze(action)
            return

        scrollbar = self.view.verticalScrollBar()

        if value >= scrollbar.maximum() - 1:
            # Deliberately not gated on `self.model.row_count > 0`: a sparse filter can leave the
            # window with zero matching rows at all, and that must still be able to detect "we've
            # caught all the way up" via _at_live_edge()'s anchor_seq fallback - otherwise a
            # zero-match window can never resume live mode (mirrors LogViewerWidget's history
            # catch-up, which doesn't gate on any matched-row count either).
            if self._at_live_edge():
                # Caught all the way up to the live edge - resume tailing.
                live_action = self._playback.handle(
                    FollowEvent.ScrolledToLiveEdge(), self._clock_snapshot(self._clock())
                )
                if live_action.kind is FollowActionKind.FETCH_LIVE:
                    self._go_live()
                return

            last_seq = self._history_newest_ref_seq()
            if last_seq is not None:
                # Deliberately not gated on `last_seq != self.model.anchor_seq`: under a sparse
                # filter, the "after" fetch can come back empty (last_seq == anchor_seq) even
                # though the backend has moved on, since none of the new raw rows matched at
                # fetch time. A `!=` guard here would then never re-scan, since history mode's
                # own apply_updates() is frozen and this scroll check is the only place that
                # re-examines the pool (mirrors LogViewerWidget._on_scroll_value_changed).
                self._reanchor_history(last_seq)
                return

        if value <= scrollbar.minimum() + 1 and self.model.row_count > 0:
            top_seq = self.model.seq_for_row(0)
            if top_seq is not None and top_seq > dtypes.SEQ_START and top_seq != self.model.anchor_seq:
                # Near the top of the fetched window with more history potentially available -
                # slide the window further back, keeping the same row under the viewport's top.
                self._reanchor_history(top_seq)

    def closeEvent(self, event):
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)
