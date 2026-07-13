# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from enum import IntEnum
from time import time_ns
from typing import TYPE_CHECKING

import numpy as np
from qtpy.QtCore import QAbstractTableModel, QEvent, QModelIndex, QSize, QSortFilterProxyModel, Qt
from qtpy.QtGui import QAction, QColor, QFont
from qtpy.QtWidgets import (
    QActionGroup,
    QApplication,
    QComboBox,
    QHeaderView,
    QLineEdit,
    QMenu,
    QSplitter,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QTableView,
    QToolBar,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE, SEQ_START
from blinkview.core.module_snapshot import MAX_MSG_BYTES
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.segments import filter_segment, nb_segment_extract_fields, nb_segment_filter_reversed
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from blinkview.utils.utc_offset import get_local_utc_offset_seconds

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

ROW_HEIGHT = 16


class LogTableCol(IntEnum):
    TIMESTAMP = 0
    RX_TIMESTAMP = 1
    DEVICE = 2
    LEVEL = 3
    MODULE = 4
    MESSAGE = 5


class LogTableModel(QAbstractTableModel):
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
    """

    HISTORY_BEFORE = 300
    HISTORY_AFTER = 300

    def __init__(self, gui_context, log_filter: LogFilter, filter_sidebar, parent=None):
        super().__init__(parent)
        self.gui_context: GUIContext = gui_context
        self.log_filter = log_filter
        self.filter_sidebar = filter_sidebar

        self.viewport_rows = 100  # LIVE mode fetch bound; the widget keeps this in sync with the view's height

        self.capacity = max(self.viewport_rows, self.HISTORY_BEFORE + self.HISTORY_AFTER)

        self.mode = "live"
        self.anchor_seq = None

        # row_count rows are valid, starting at self._valid_start within the fixed-size arrays
        # below (avoids ever having to shift/copy data to "row 0" after a rebuild).
        self.row_count = 0
        self._valid_start = self.capacity

        # Preallocated once (never inside apply_updates) from the shared array pool, mirroring
        # how PooledLogBatch is used elsewhere for segment storage. nb_segment_extract_fields
        # writes directly into .bundle's columns; offsets/lengths are written fixed-stride
        # (row * MAX_MSG_BYTES) rather than via the running msg_cursor used by ingestion.
        array_pool = self.gui_context.registry.system_ctx.array_pool
        self.batch = array_pool.create(
            PooledLogBatch,
            self.capacity,
            self.capacity * MAX_MSG_BYTES,
            has_levels=True,
            has_modules=True,
            has_devices=True,
            has_sequences=True,
        )
        bundle = self.batch.bundle
        self.ts = bundle.timestamps
        self.rx_ts = bundle.rx_timestamps
        self.dev = bundle.devices
        self.lvl = bundle.levels
        self.mod = bundle.modules
        self.seq = bundle.sequences
        self.msg_offsets = bundle.offsets
        self.msg_buffer = bundle.buffer
        self.msg_lengths = bundle.lengths
        self._message_cache = [None] * self.capacity

        self._tz_offset_ns = get_local_utc_offset_seconds() * 1_000_000_000
        self.ts_precision = 3  # 0=seconds, 3=milliseconds, 6=microseconds, 9=nanoseconds

        self._prev_total_module_count = None
        self._filter_cache = None
        self._effective_mask = None

        # Sentinel forces a rebuild on the very first live tick regardless of the backend's sequence.
        self._last_backend_seq = None
        self.prev_apply = 0

        self.logger = self.gui_context.logger.child("log_table", enabled=True)
        self.logger_fetch = self.logger.child("fetch")

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Exercises nb_segment_extract_fields, nb_segment_filter_reversed and filter_segment -
        the kernels this widget uses that aren't already warmed by NumbaWarmupHelper's other
        exercise_* methods - using the helper's dummy log_pool/registry instead of standing up
        a real LogTableModel. Numba compiles a distinct specialization per call-site signature
        (e.g. end_seq/start_seq passed vs. left at its default), so every call shape actually
        used by _fetch_live/_fetch_history is exercised here too - matching only one shape
        leaves the others to JIT-compile (a stall) on their first real call instead."""
        print("[Warmup] LogTableModel ...")

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
            ) as out_batch,
            helper.log_pool.get_reversed_snapshot() as segments,
            helper.log_pool.acquire_indices_buffer() as indices,
        ):
            for segment in segments:
                if segment.size == 0:
                    continue

                # _fetch_live's shape: no end_seq.
                match_count = nb_segment_filter_reversed(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=capacity,
                )

                # _fetch_history's "before" shape: end_seq passed explicitly.
                nb_segment_filter_reversed(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=capacity,
                    end_seq=dtypes.SEQ_TYPE(SEQ_START),
                )

                # _fetch_history's "after" shape: filter_segment with start_seq passed explicitly.
                filter_segment(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=capacity,
                    start_seq=SEQ_NONE,
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

        print("[Warmup] LogTableModel ... done")

    # --- Qt model plumbing ---------------------------------------------------------------

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return self.row_count

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(LogTableCol)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            names = {
                LogTableCol.TIMESTAMP: "Time",
                LogTableCol.RX_TIMESTAMP: "Rx Time",
                LogTableCol.DEVICE: "Device",
                LogTableCol.LEVEL: "Level",
                LogTableCol.MODULE: "Module",
                LogTableCol.MESSAGE: "Message",
            }
            return names.get(LogTableCol(section))
        return super().headerData(section, orientation, role)

    def set_ts_precision(self, precision: int):
        if self.ts_precision == precision:
            return
        self.ts_precision = precision
        if self.row_count > 0:
            top_left = self.index(0, LogTableCol.TIMESTAMP)
            bottom_right = self.index(self.row_count - 1, LogTableCol.RX_TIMESTAMP)
            self.dataChanged.emit(top_left, bottom_right)

    def _row_to_slot(self, row: int) -> int:
        """Maps a model row (0 = oldest currently shown) to its slot in the fixed-size arrays."""
        return self._valid_start + row

    def _format_ts(self, ts_ns: int) -> str:
        ns = ts_ns + self._tz_offset_ns
        total_sec = ns // 1_000_000_000
        sec = total_sec % 60
        minute = (total_sec // 60) % 60
        hour = (total_sec // 3600) % 24
        base = f"{hour:02d}:{minute:02d}:{sec:02d}"

        precision = self.ts_precision
        if precision <= 0:
            return base

        sub_ns = ns % 1_000_000_000
        if precision >= 9:
            frac = f"{sub_ns:09d}"
        elif precision >= 6:
            frac = f"{sub_ns // 1_000:06d}"
        else:
            frac = f"{sub_ns // 1_000_000:03d}"

        return f"{base}.{frac}"

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        if row >= self.row_count:
            return None

        slot = self._row_to_slot(row)

        if role == Qt.ForegroundRole:
            level = LogLevel.from_value(int(self.lvl[slot]))
            return QColor(level.color) if level else None

        if role == Qt.FontRole:
            level = LogLevel.from_value(int(self.lvl[slot]))
            if level and level >= LogLevel.WARN:
                font = QFont()
                font.setBold(True)
                return font
            return None

        if role != Qt.DisplayRole:
            return None

        col = index.column()
        if col == LogTableCol.TIMESTAMP:
            return self._format_ts(int(self.ts[slot]))
        if col == LogTableCol.RX_TIMESTAMP:
            return self._format_ts(int(self.rx_ts[slot]))
        if col == LogTableCol.DEVICE:
            device = self.gui_context.id_registry.devices.get(int(self.dev[slot]))
            return device.name if device else "?"
        if col == LogTableCol.LEVEL:
            level = LogLevel.from_value(int(self.lvl[slot]))
            return level.name_conf if level else "?"
        if col == LogTableCol.MODULE:
            module = self.gui_context.id_registry.modules.get(int(self.mod[slot]))
            return module.name if module else "?"
        if col == LogTableCol.MESSAGE:
            return self._decode_message(slot)

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

        msg_len = int(self.msg_lengths[slot])
        off = int(self.msg_offsets[slot])
        text = self.msg_buffer[off : off + msg_len].tobytes().decode("utf-8", errors="replace")
        self._message_cache[slot] = text
        return text

    # --- Mode management ------------------------------------------------------------------

    def set_viewport_rows(self, n: int):
        """Called by the widget when the view is resized, so LIVE mode fetches exactly enough
        rows to fill the visible area - never more, since LIVE mode has no scrollbar to reach
        any rows beyond what fits on screen."""
        n = max(1, n)
        if n == self.viewport_rows:
            return
        self.viewport_rows = n
        if self.mode == "live":
            self._last_backend_seq = None  # force a refetch at the new size
            self._fetch_live(force=True)

    def enter_live_mode(self):
        self.mode = "live"
        self.anchor_seq = None
        self._last_backend_seq = None
        self._fetch_live(force=True)

    def enter_history_mode(self, anchor_seq):
        if anchor_seq is None:
            return
        self.mode = "history"
        self.anchor_seq = anchor_seq
        self._bake_effective_mask()
        self._fetch_history(anchor_seq)

    def clear_logs(self):
        self.beginResetModel()
        self.row_count = 0
        self._valid_start = self.capacity
        self._message_cache = [None] * self.capacity
        log_pool = self.gui_context.registry.central.log_pool
        self._last_backend_seq = log_pool.latest_sequence()
        self.mode = "live"
        self.anchor_seq = None
        self.endResetModel()

    def reload_and_redraw(self):
        """Forces a re-fetch under the current mode (used when filter settings change - same
        data, different criteria). Stays anchored to the same position in history mode."""
        self._effective_mask = None
        if self.mode == "history" and self.anchor_seq is not None:
            self._bake_effective_mask()
            self._fetch_history(self.anchor_seq)
        else:
            self._last_backend_seq = None
            self._fetch_live(force=True)

    # --- Filtering --------------------------------------------------------------------------

    def _bake_effective_mask(self):
        reg = self.gui_context.id_registry
        f = self.log_filter

        if self._prev_total_module_count != (mod_count := reg.module_count()) or self._filter_cache is None:
            self._prev_total_module_count = mod_count
            self._effective_mask = None

            if m := f.filtered_module:
                t_list = (
                    reg.get_descendant_ids(m.id)
                    if f.filtered_module_children
                    else np.array([m.id], dtype=dtypes.ID_TYPE)
                )
            elif dev := f.allowed_device:
                t_list = f.allowed_device.get_all_module_ids()
            else:
                t_list = None

            self._filter_cache = t_list

        if self._effective_mask is None or len(self._effective_mask) < mod_count:
            filter_enabled, sidebar_mask = self.filter_sidebar.get_filter()
            global_threshold = dtypes.LEVEL_TYPE(f.log_level.value)

            if filter_enabled:
                mask_to_use = sidebar_mask[:mod_count] if len(sidebar_mask) >= mod_count else sidebar_mask
                raw_effective = np.maximum(mask_to_use, global_threshold)
                if self._filter_cache is not None:
                    self._effective_mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
                    self._effective_mask[self._filter_cache] = raw_effective[self._filter_cache]
                else:
                    self._effective_mask = raw_effective
            else:
                show_hidden = self.filter_sidebar.action_show_non_essential.isChecked()

                if show_hidden:
                    self._effective_mask = np.full(mod_count, global_threshold, dtype=dtypes.LEVEL_TYPE)
                else:
                    essential_mask = reg._essential_array[:mod_count]
                    self._effective_mask = np.where(essential_mask, global_threshold, LogLevel.OFF.value).astype(
                        dtypes.LEVEL_TYPE
                    )

                if self._filter_cache is not None:
                    mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
                    mask[self._filter_cache] = self._effective_mask[self._filter_cache]
                    self._effective_mask = mask

    # --- LIVE mode fetch --------------------------------------------------------------------

    def apply_updates(self):
        """Driven by the GUIContext heartbeat (~10Hz). Only does anything in LIVE mode -
        HISTORY mode is a static snapshot until the widget calls enter_live_mode()/
        enter_history_mode() again in response to scrolling."""
        if self.mode != "live":
            return

        now = time_ns()
        if now - self.prev_apply < 100_000_000:  # Throttle to ~10Hz
            return
        self.prev_apply = now

        self._fetch_live()

    def _fetch_live(self, force: bool = False):
        pool = self.gui_context.registry.central.log_pool
        current_backend_seq = pool.latest_sequence()

        if not force and current_backend_seq == self._last_backend_seq:
            return  # Nothing new in the backend since the last fetch - skip entirely.
        self._last_backend_seq = current_backend_seq

        self.filter_sidebar.sync_modules()
        self._bake_effective_mask()

        limit = self.viewport_rows
        write_cursor = self.capacity
        total_new = 0
        segment_count = 0

        t_start = time_ns()

        with pool.get_reversed_snapshot() as segments, pool.acquire_indices_buffer() as indices:
            for segment in segments:
                if segment.size == 0:
                    continue

                allowed = limit - total_new
                if allowed <= 0:
                    break

                segment_count += 1

                match_count = nb_segment_filter_reversed(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    max_matches=allowed,
                )

                if match_count > 0:
                    write_cursor -= match_count
                    nb_segment_extract_fields(
                        segment.bundle,
                        indices.array,
                        match_count,
                        self.batch.bundle,
                        write_cursor,
                        MAX_MSG_BYTES,
                    )
                    total_new += match_count

        self.logger_fetch.debug(
            f"live {(time_ns() - t_start) / 1_000_000:.3f} ms | segments={segment_count} rows={total_new}"
        )

        self.beginResetModel()
        self._valid_start = write_cursor
        self.row_count = total_new
        self._message_cache = [None] * self.capacity
        self.endResetModel()

    # --- HISTORY mode fetch -------------------------------------------------------------------

    def _fetch_history(self, anchor_seq: int):
        pool = self.gui_context.registry.central.log_pool
        boundary = self.HISTORY_BEFORE  # fixed split point between the "before" and "after" regions

        before_count = 0
        after_count = 0

        t_start = time_ns()

        # "Before" set: matches with seq <= anchor_seq - 1, closest to the anchor first, scanning
        # newest-to-oldest and writing right-aligned into [0:boundary) - same "fill from the right
        # end inward" trick used elsewhere so multi-segment results land in order with no copying.
        if anchor_seq is not None and anchor_seq > SEQ_START:
            write_cursor = boundary
            with pool.get_reversed_snapshot() as segments, pool.acquire_indices_buffer() as indices:
                for segment in segments:
                    if segment.size == 0:
                        continue

                    allowed = self.HISTORY_BEFORE - before_count
                    if allowed <= 0:
                        break

                    match_count = nb_segment_filter_reversed(
                        segment.bundle,
                        effective_mask=self._effective_mask,
                        out_indices=indices.array,
                        max_matches=allowed,
                        end_seq=dtypes.SEQ_TYPE(anchor_seq - 1),
                    )

                    if match_count > 0:
                        write_cursor -= match_count
                        nb_segment_extract_fields(
                            segment.bundle,
                            indices.array,
                            match_count,
                            self.batch.bundle,
                            write_cursor,
                            MAX_MSG_BYTES,
                        )
                        before_count += match_count

        # "After" set: matches with seq >= anchor_seq, ascending, scanning oldest-to-newest and
        # writing left-aligned starting at the fixed boundary.
        start_seq_lower_bound = dtypes.SEQ_TYPE(
            SEQ_NONE if anchor_seq is None or anchor_seq <= SEQ_START else anchor_seq - 1
        )
        write_cursor = boundary
        with pool.get_snapshot() as segments, pool.acquire_indices_buffer() as indices:
            for segment in segments:
                if segment.size == 0:
                    continue

                allowed = self.HISTORY_AFTER - after_count
                if allowed <= 0:
                    break

                match_count = filter_segment(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    max_matches=allowed,
                    start_seq=start_seq_lower_bound,
                )

                if match_count > 0:
                    nb_segment_extract_fields(
                        segment.bundle,
                        indices.array,
                        match_count,
                        self.batch.bundle,
                        write_cursor,
                        MAX_MSG_BYTES,
                    )
                    write_cursor += match_count
                    after_count += match_count

        self.logger_fetch.debug(
            f"history {(time_ns() - t_start) / 1_000_000:.3f} ms | before={before_count} after={after_count}"
        )

        self.beginResetModel()
        self._valid_start = boundary - before_count
        self.row_count = before_count + after_count
        self._message_cache = [None] * self.capacity
        self.endResetModel()


class LogTablePlainTextDelegate(QStyledItemDelegate):
    """Draws cell text directly instead of going through QStyledItemDelegate's default paint path,
    which auto-detects rich text (anything with '<') and lays it out via QTextDocument on every
    repaint - log messages routinely contain '<'/'>' (comparisons, tags, macros), making that
    default path a per-cell, per-repaint cost across the whole visible viewport. Mirrors
    TelemetryDelegate, which exists in this codebase for the exact same reason."""

    def paint(self, painter, option, index):
        painter.save()

        if option.state & QStyle.State_Selected:
            painter.fillRect(option.rect, option.palette.highlight())
            painter.setPen(option.palette.highlightedText().color())
        else:
            if option.features & QStyleOptionViewItem.ViewItemFeature.Alternate:
                painter.fillRect(option.rect, option.palette.alternateBase())
            fg = index.data(Qt.ForegroundRole)
            painter.setPen(fg if fg is not None else option.palette.text().color())

        font = index.data(Qt.FontRole)
        if font is not None:
            painter.setFont(font)

        text = index.data(Qt.DisplayRole)
        text_rect = option.rect.adjusted(4, 0, -4, 0)
        painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, "" if text is None else str(text))

        painter.restore()

    def sizeHint(self, option, index):
        # Bypass QStyledItemDelegate's font-metric-based size calculation entirely - row height
        # is fixed by the view's vertical header anyway.
        return QSize(50, ROW_HEIGHT)


class LogTableFilterProxy(QSortFilterProxyModel):
    """Text-search proxy over LogTableModel, mirroring TelemetryTable's search box behavior."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._filter_text = ""

    def set_filter_text(self, text: str):
        self._filter_text = text.lower().strip()
        self.invalidate()

    def filterAcceptsRow(self, source_row, source_parent):
        if not self._filter_text:
            return True

        model = self.sourceModel()
        for col in (LogTableCol.DEVICE, LogTableCol.MODULE, LogTableCol.MESSAGE):
            idx = model.index(source_row, col, source_parent)
            val = model.data(idx, Qt.DisplayRole)
            if val and self._filter_text in str(val).lower():
                return True

        return False


class LogTableViewerWidget(QWidget):
    """Table-based alternative to LogViewerWidget, sharing the same LogFilter/ModuleFilterSidebar
    filtering capability but rendering structured rows in a QTableView instead of formatted text.

    Runs in two tiers (see LogTableModel): a scrollbar-less LIVE tail view that only fetches
    enough rows to fill the visible area, and a bounded HISTORY view (entered on scroll) that
    fetches a couple hundred rows before/after the point the user scrolled away from."""

    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)

        self.gui_context: GUIContext = gui_context

        self.tab_name = ""
        self.allowed_device = None
        self.filtered_module = None
        self.filtered_module_children = False
        self.log_level = LogLevel.ALL.name_conf
        self.filter_sidebar_state = None
        self.show_module_filter = False
        self.show_hidden = False
        self.show_rx_ts = False
        self.ts_precision = 3

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

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Filter device/module/message...")
        self.search_box.setClearButtonEnabled(True)
        self.search_box.textChanged.connect(self._on_search_changed)
        self.toolbar.addWidget(self.search_box)

        self.toolbar.addSeparator()

        self.action_clear = QAction("Clear", self)
        self.action_clear.triggered.connect(self.clear_logs)
        self.toolbar.addAction(self.action_clear)

        self.action_go_live = QAction("⏵ Go Live", self)
        self.action_go_live.setToolTip("Return to the live tail (jump back to the latest rows)")
        self.action_go_live.triggered.connect(self._go_live)
        self.action_go_live.setVisible(False)
        self.toolbar.addAction(self.action_go_live)

        self.splitter = QSplitter(Qt.Horizontal, self)
        self.layout.addWidget(self.splitter)

        self.log_filter = LogFilter(
            self.gui_context.id_registry,
            self.allowed_device,
            self.filtered_module,
            log_level=self.log_level,
            filtered_module_children=self.filtered_module_children,
        )

        self.filter_sidebar = ModuleFilterSidebar(
            gui_context=self.gui_context, target_filter=self.log_filter, parent=self, show_hidden=self.show_hidden
        )
        self.filter_sidebar.restore_state(self.filter_sidebar_state)
        self.filter_sidebar.log_filter.filter_changed.connect(self._reload_and_redraw)
        self.filter_sidebar.action_enable.toggled.connect(lambda _checked: self._reload_and_redraw())
        self.filter_sidebar.setMinimumWidth(200)
        self.splitter.addWidget(self.filter_sidebar)
        self.filter_sidebar.setVisible(self.show_module_filter)

        self.model = LogTableModel(self.gui_context, self.log_filter, self.filter_sidebar, self)

        self.proxy = LogTableFilterProxy(self)
        self.proxy.setSourceModel(self.model)

        self.view = QTableView()
        self.view.setModel(self.proxy)
        self.view.setSelectionBehavior(QTableView.SelectRows)
        self.view.horizontalHeader().setStretchLastSection(True)
        # self.view.setAlternatingRowColors(True)
        self.view.setMinimumWidth(300)
        self.view.setWordWrap(False)
        self.view.setItemDelegate(LogTablePlainTextDelegate(self.view))

        # Fixed row height avoids Qt recomputing per-row geometry for the whole table on every
        # change (an O(row_count) relayout cost) - same pattern TelemetryTable uses.
        v_header = self.view.verticalHeader()
        v_header.setVisible(False)
        v_header.setSectionResizeMode(QHeaderView.Fixed)
        v_header.setDefaultSectionSize(ROW_HEIGHT)

        # LIVE mode has nothing to scroll to (only enough rows are fetched to fill the view).
        self.view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self.model.ts_precision = self.ts_precision
        self.view.setColumnHidden(LogTableCol.RX_TIMESTAMP, not self.show_rx_ts)

        self.view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.view.customContextMenuRequested.connect(self._show_context_menu)

        self._programmatic_scroll = False
        self.view.viewport().installEventFilter(self)
        self.view.verticalScrollBar().valueChanged.connect(self._on_scroll_value_changed)

        self.splitter.addWidget(self.view)
        self.splitter.setStretchFactor(0, 2)
        self.splitter.setStretchFactor(1, 8)

        show_filter_btn = self.filtered_module is None or self.filtered_module_children
        self.action_toggle_filter.setVisible(show_filter_btn)

        idx = self.level_combo.findData(LogLevel.from_string(self.log_level))
        if idx != -1:
            self.level_combo.setCurrentIndex(idx)

        self._update_viewport_rows()

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
        self.show_rx_ts = view_state.get("show_rx_ts", self.show_rx_ts)
        self.ts_precision = view_state.get("ts_precision", self.ts_precision)
        self.filter_sidebar_state = state.get("filter_sidebar", self.filter_sidebar_state)

    def get_state(self):
        return {
            "tab_name": self.tab_name,
            "allowed_device": self.allowed_device.name if self.allowed_device else None,
            "filtered_module": f"{self.filtered_module.name_with_device()}" if self.filtered_module else None,
            "filtered_module_children": self.filtered_module_children,
            "view_state": {
                "show_module_filter": self.show_module_filter,
                "show_rx_ts": self.show_rx_ts,
                "ts_precision": self.ts_precision,
            },
            "log_level": self.log_filter.log_level.name_conf,
            "filter_sidebar": self.filter_sidebar.get_state(),
            "show_hidden": self.filter_sidebar.action_show_non_essential.isChecked(),
        }

    def apply_updates(self):
        self.model.apply_updates()

    def _handle_level_change(self, index):
        level_identity = self.level_combo.itemData(index)
        self.log_filter.set_level(level_identity.name_conf)
        self._reload_and_redraw()

    def _reload_and_redraw(self):
        self.model.reload_and_redraw()

    def _on_search_changed(self, text):
        self.proxy.set_filter_text(text)

    def _toggle_module_filter(self, checked):
        self.show_module_filter = checked
        self.filter_sidebar.setVisible(checked)

    def _toggle_rx_ts(self, checked):
        self.show_rx_ts = checked
        self.view.setColumnHidden(LogTableCol.RX_TIMESTAMP, not checked)

    def _set_ts_precision(self, precision: int):
        if self.ts_precision != precision:
            self.ts_precision = precision
            self.model.set_ts_precision(precision)

    def clear_logs(self):
        self.model.clear_logs()
        self._set_live_ui_state()

    # --- Live/history mode transitions -----------------------------------------------------

    def _update_viewport_rows(self):
        """Keeps LIVE mode's fetch bound matched to how many rows actually fit on screen, so it
        never fetches (or holds) more than the visible area needs. LIVE mode has no scrollbar
        (ScrollBarAlwaysOff), so fetching even one row more than truly fits pushes the newest
        row(s) below the viewport with no way to reach them - must be an exact floor, not a
        margin over-fetch.

        Divides by the header's actual effective row height, not the ROW_HEIGHT constant:
        QHeaderView.setDefaultSectionSize() silently clamps up to minimumSectionSize() if the
        requested size is smaller than what the header's font metrics need (system
        font/DPI-dependent) - dividing by the constant in that case would over-count how many
        rows fit and push the newest ones below the viewport."""
        row_height = self.view.verticalHeader().defaultSectionSize() or ROW_HEIGHT
        rows = self.view.viewport().height() // row_height
        self.model.set_viewport_rows(rows)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_viewport_rows()

    def eventFilter(self, source, event):
        if source is self.view.viewport():
            if event.type() == QEvent.Wheel:
                if self.model.mode == "live" and event.angleDelta().y() > 0:
                    # Scrolling "up" (towards older entries) while live: anchor on whatever's
                    # currently on top and switch into a scrollable history window around it.
                    self._enter_history_at_top_row()
            elif event.type() == QEvent.Resize:
                # The widget's own resizeEvent fires once during initial show, often with a
                # transient intermediate size before the splitter/layout fully settles - the
                # viewport's final size is reached afterwards without re-firing it. Watching the
                # viewport's own Resize event directly catches that final size instead of
                # under-fetching rows based on a stale measurement.
                self._update_viewport_rows()
        return super().eventFilter(source, event)

    def _topmost_row_seq(self):
        top_index = self.view.indexAt(self.view.viewport().rect().topLeft())
        if not top_index.isValid():
            return self.model.seq_for_row(0)
        src_index = self.proxy.mapToSource(top_index)
        return self.model.seq_for_row(src_index.row())

    def _enter_history_at_top_row(self):
        anchor_seq = self._topmost_row_seq()
        if anchor_seq is None:
            return
        self._reanchor_history(anchor_seq)
        self._set_history_ui_state()

    def _reanchor_history(self, anchor_seq):
        """Rebuilds the history window around anchor_seq and repositions the view on it. Both
        enter_history_mode() (via beginResetModel/endResetModel) and scrollTo() below change the
        scrollbar's value programmatically, which would otherwise re-fire valueChanged and recurse
        straight back into _on_scroll_value_changed - the guard flag suppresses that re-entrancy."""
        self._programmatic_scroll = True
        try:
            self.model.enter_history_mode(anchor_seq)

            # If this fetch's "after" set already reaches the backend's latest row, there may be
            # too little remaining content to fill the viewport - the scrollbar's range can
            # collapse to "nothing to scroll", meaning valueChanged will never fire again to tell
            # us we've caught up. Check eagerly here instead of only reacting to future scrolls.
            if self._at_live_edge():
                self.model.enter_live_mode()
                self._set_live_ui_state()
                return

            # QTableView defers its internal row/geometry layout after a model reset (a 0ms
            # timer, for performance) rather than recomputing it synchronously inside
            # endResetModel(). scrollTo() called right after enter_history_mode() would then
            # run against stale geometry from the *previous* content and silently land on the
            # wrong row (observed: always row 0) instead of the anchor. Force the layout to
            # happen now so scrollTo() has correct geometry to work with.
            self.view.doItemsLayout()
            self._scroll_to_seq(anchor_seq)
        finally:
            self._programmatic_scroll = False

    def _at_live_edge(self) -> bool:
        if self.model.row_count == 0:
            return False
        last_seq = self.model.seq_for_row(self.model.row_count - 1)
        if last_seq is None:
            return False
        pool = self.gui_context.registry.central.log_pool
        return last_seq >= pool.latest_sequence()

    def _scroll_to_seq(self, seq):
        row = self.model.row_for_seq(seq)
        if row < 0:
            return
        proxy_index = self.proxy.mapFromSource(self.model.index(row, 0))
        if proxy_index.isValid():
            self.view.scrollTo(proxy_index, QTableView.PositionAtTop)

    def _go_live(self):
        self._programmatic_scroll = True
        try:
            self.model.enter_live_mode()
        finally:
            self._programmatic_scroll = False
        self._set_live_ui_state()

    def _set_live_ui_state(self):
        self.view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.action_go_live.setVisible(False)

    def _set_history_ui_state(self):
        self.view.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.action_go_live.setVisible(True)

    def _on_scroll_value_changed(self, value):
        if self._programmatic_scroll:
            return

        if self.model.mode != "history":
            return

        scrollbar = self.view.verticalScrollBar()

        if value >= scrollbar.maximum() - 1 and self.model.row_count > 0:
            if self._at_live_edge():
                # Caught all the way up to the live edge - resume tailing.
                self._go_live()
                return

            last_seq = self.model.seq_for_row(self.model.row_count - 1)
            if last_seq is not None and last_seq != self.model.anchor_seq:
                # More rows exist in the backend beyond what this (static) window fetched -
                # slide the window forward, anchored on the last row so it becomes the top
                # of the next chunk (continuous scroll instead of dead-ending mid-history).
                self._reanchor_history(last_seq)
                return

        if value <= scrollbar.minimum() + 1 and self.model.row_count > 0:
            top_seq = self.model.seq_for_row(0)
            if top_seq is not None and top_seq > SEQ_START and top_seq != self.model.anchor_seq:
                # Near the top of the fetched window with more history potentially available -
                # slide the window further back, keeping the same row under the viewport's top.
                self._reanchor_history(top_seq)

    def _show_context_menu(self, pos):
        index = self.view.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        action_copy = QAction("Copy Message", self)
        action_copy.triggered.connect(lambda: self._copy_message(index))
        menu.addAction(action_copy)
        menu.exec_(self.view.viewport().mapToGlobal(pos))

    def _copy_message(self, index):
        src_index = self.proxy.mapToSource(index)
        row = src_index.row()
        if 0 <= row < self.model.row_count:
            slot = self.model._row_to_slot(row)
            QApplication.clipboard().setText(self.model._decode_message(slot))

    def closeEvent(self, event):
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)
