# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Qt-independent core shared by LogViewerWidget, LogTableStore, and ConsoleSubscriber: bakes an
effective_mask from a LogFilter + module registry, and scans log_pool segments (a live tail bound
by a sequence watermark, or a before/after window around an anchor) for rows passing it.

LogSegmentScanner owns mask-baking and the segment-iteration/bounds mechanics only - the terminal
"what do I do with each match" step (format to text via nb_segment_format, or extract into a
columnar PooledLogBatch via nb_segment_extract_fields) is supplied by the caller as a `consume`
callback, so each output shape stays a short, concrete function rather than a branch inside this
class. LogTextFetcher is a thin convenience wrapper around it for the two text-output consumers
(LogViewerWidget, ConsoleSubscriber).

LogTableStore (ui/widgets/log_table_viewer.py) is migrated onto LogSegmentScanner directly (no
wrapper needed) - its own ping-pong-buffer/incremental-vs-full-rescan dispatch and
nb_segment_extract_fields terminal write stay local to that class, just delegating segment
iteration to scan_tail/scan_history_window.
"""

from typing import TYPE_CHECKING, Callable, NamedTuple, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.types.formatting import FormattingConfig
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.formatting import nb_segment_estimate_out_size, nb_segment_format
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS
from blinkview.ops.segments import segment_filter, segment_filter_reversed
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH
from blinkview.utils.log_level import LogLevel

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper
    from blinkview.utils.log_filter import LogFilter

# (segment, out_indices_array, match_count) -> None. Caller writes the matched rows wherever it
# wants (accumulating text batches, a columnar buffer at some write cursor, ...).
MatchConsumer = Callable[[object, np.ndarray, int], None]


class LiveScanResult(NamedTuple):
    total_new_rows: int
    reached_live_edge: bool
    highest_seq_seen: int


class HistoryScanResult(NamedTuple):
    before_count: int
    after_count: int
    oldest_seq: Optional[int]
    newest_seq: Optional[int]
    reached_start: bool


class LogSegmentScanner:
    """Shared mask-baking + segment-scan core. `start_seq`/watermark bookkeeping is deliberately
    left to the caller (passed into scan_tail explicitly, returned via highest_seq_seen) rather
    than owned here - LogViewerWidget/ConsoleSubscriber track one continuously-advancing
    watermark, while LogTableStore tracks its own (_last_backend_seq) and additionally needs a
    from-scratch full rescan (start_seq=SEQ_NONE) on filter changes, which isn't expressible if
    this class owned a single monotonic watermark itself."""

    def __init__(
        self,
        id_registry,
        get_log_pool: Callable[[], object],
        log_filter: "LogFilter",
        get_sidebar_filter: Callable[[], tuple],
        get_show_hidden: Callable[[], bool],
    ):
        self.id_registry = id_registry
        # A callable, not a captured pool reference - GUIContext.registry.central.log_pool is
        # read fresh on every scan rather than once at construction time, matching the original
        # per-consumer code (LogViewerWidget/LogTableStore both did `pool =
        # self.gui_context.registry.central.log_pool` inline on every fetch call, not in
        # __init__) - tests routinely swap in a new FakeLogPool mid-test, which a captured
        # reference would silently miss.
        self.get_log_pool = get_log_pool
        self.log_filter = log_filter
        self.get_sidebar_filter = get_sidebar_filter
        self.get_show_hidden = get_show_hidden

        self._prev_total_module_count = None
        self._filter_cache = None
        self._filter_cache_computed = False
        self._effective_mask = None

    def invalidate_mask(self) -> None:
        self._effective_mask = None

    @property
    def effective_mask(self) -> Optional[np.ndarray]:
        """Read-only access to the currently baked mask, for callers that need its object
        identity for their own caching (e.g. LogTableStore's filter-changed detection deciding
        between a full rescan and a cheap incremental one) - internal scan methods always call
        ensure_effective_mask() themselves and never rely on a caller having read this first."""
        return self._effective_mask

    def ensure_effective_mask(self) -> int:
        """Bakes self._effective_mask/self._filter_cache from the current module registry and
        filter sidebar state. Shared by every scan below so live/history fetches always see
        identical filtering. `_filter_cache_computed` (rather than a bare `is None` check on
        `_filter_cache`) is needed because `_filter_cache` legitimately holds None as its
        steady-state value (no module/device filter active)."""
        reg = self.id_registry
        f = self.log_filter

        if self._prev_total_module_count != (mod_count := reg.module_count()) or not self._filter_cache_computed:
            self._prev_total_module_count = mod_count
            self._filter_cache_computed = True
            self._effective_mask = None  # Registry grew, invalidate the mask

            if m := f.filtered_module:
                t_list = (
                    reg.get_descendant_ids(m.id)
                    if f.filtered_module_children
                    else np.array([m.id], dtype=dtypes.ID_TYPE)
                )
            elif dev := f.allowed_device:
                # Tab is restricted to a device (No specific module)
                t_list = f.allowed_device.get_all_module_ids()
            else:
                # Global 'All Logs' view
                t_list = None

            self._filter_cache = t_list

        # --- Bake Effective Mask (ONLY IF INVALID) ---
        if self._effective_mask is None or len(self._effective_mask) < mod_count:
            filter_enabled, sidebar_mask = self.get_sidebar_filter()
            global_threshold = dtypes.LEVEL_TYPE(f.log_level.value)

            if filter_enabled:
                # Path 1: Surgical Mode
                mask_to_use = sidebar_mask[:mod_count] if len(sidebar_mask) >= mod_count else sidebar_mask
                raw_effective = np.maximum(mask_to_use, global_threshold)
                if self._filter_cache is not None:
                    self._effective_mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
                    self._effective_mask[self._filter_cache] = raw_effective[self._filter_cache]
                else:
                    self._effective_mask = raw_effective
            else:
                # Path 2: Tab Fallback Mode
                show_hidden = self.get_show_hidden()

                if show_hidden:
                    # Show everything up to the global threshold
                    self._effective_mask = np.full(mod_count, global_threshold, dtype=dtypes.LEVEL_TYPE)
                else:
                    essential_mask = reg._essential_array[:mod_count]
                    # Apply threshold only to essential modules
                    self._effective_mask = np.where(essential_mask, global_threshold, LogLevel.OFF.value).astype(
                        dtypes.LEVEL_TYPE
                    )

                if self._filter_cache is not None:
                    # Constrain to the tab's allowed cache
                    mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
                    mask[self._filter_cache] = self._effective_mask[self._filter_cache]
                    self._effective_mask = mask

        return mod_count

    def scan_tail(self, *, start_seq: int, max_rows: int, consume: MatchConsumer) -> LiveScanResult:
        """Scans pool.get_reversed_snapshot() (newest-to-oldest segments) for rows with
        seq > start_seq passing the current effective_mask/kv/text filter, invoking
        `consume(segment, indices_array, match_count)` per non-empty match, until either the
        pool is exhausted or `max_rows` matches have been found.

        `start_seq=SEQ_NONE` scans everything (a "from scratch" full rescan bounded only by
        max_rows/quota) - segment.last_sequence_id <= SEQ_NONE(0) is never true for real
        sequences (SEQ_START=1), so the early-exit below degenerates to "never break early",
        which is exactly a full rescan's desired behavior. A real watermark bounds the scan to
        genuinely new rows the same way.

        Returns LiveScanResult(total_new_rows, reached_live_edge, highest_seq_seen) -
        highest_seq_seen is the newest sequence actually evaluated (not necessarily matched),
        i.e. the next watermark a caller tracking one should advance to."""
        self.ensure_effective_mask()
        kv = self.log_filter.bake_kv_arrays()
        text = self.log_filter.bake_text_search()

        total_new_rows = 0
        reached_live_edge = True
        highest_seq_seen = start_seq
        first_segment = True

        log_pool = self.get_log_pool()
        with log_pool.get_reversed_snapshot() as segments, log_pool.acquire_indices_buffer() as indices:
            for segment in segments:
                segment_last_sequence_id = segment.last_sequence_id

                # Iterating backwards (newest to oldest segments): once a segment's LAST
                # sequence is <= our watermark, every remaining segment is guaranteed older too.
                if segment.size == 0 or segment_last_sequence_id <= start_seq:
                    break

                if first_segment:
                    highest_seq_seen = segment_last_sequence_id
                    first_segment = False

                allowed_matches = max_rows - total_new_rows
                match_count = segment_filter_reversed(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    max_matches=allowed_matches,
                    start_seq=start_seq,
                    kv=kv,
                    text=text,
                )

                if match_count > 0:
                    consume(segment, indices.array, match_count)
                    total_new_rows += match_count

                if total_new_rows >= max_rows:
                    reached_live_edge = False
                    break

        return LiveScanResult(total_new_rows, reached_live_edge, highest_seq_seen)

    def scan_history_window(
        self,
        *,
        anchor_seq: Optional[int] = None,
        anchor_ts: Optional[int] = None,
        before_cap: int,
        after_cap: int,
        consume_before: MatchConsumer,
        consume_after: MatchConsumer,
    ) -> HistoryScanResult:
        """Fetches a bounded window of matching rows before/after an anchor point (mutually
        exclusive: a sequence id for manual scroll/pause paging, or a timestamp for
        playback-clock following). `consume_before`/`consume_after` are invoked per matching
        segment in scan order (before: newest-to-oldest; after: oldest-to-newest) - kept as two
        separate callbacks rather than one because callers write in opposite directions (e.g. a
        right-to-left write cursor for "before", left-to-right for "after") and need to know
        which phase they're in.

        end_ts is an inclusive upper bound like end_seq, but start_ts is an inclusive lower
        bound (unlike start_seq, which is exclusive) - hence the asymmetric `anchor_ts` vs
        `anchor_ts - 1` between the two scans below.
        """
        self.ensure_effective_mask()
        kv = self.log_filter.bake_kv_arrays()
        text = self.log_filter.bake_text_search()

        has_seq_anchor = anchor_seq is not None and anchor_seq > dtypes.SEQ_START
        has_ts_anchor = anchor_ts is not None

        # --- "Before" set: matches strictly before the anchor, scanning newest-to-oldest ---
        before_count = 0
        oldest_seq = anchor_seq if has_seq_anchor else None

        log_pool = self.get_log_pool()

        if has_seq_anchor or has_ts_anchor:
            before_kwargs = {"end_seq": anchor_seq - 1} if has_seq_anchor else {"end_ts": anchor_ts - 1}
            with log_pool.get_reversed_snapshot() as segments, log_pool.acquire_indices_buffer() as indices:
                for segment in segments:
                    if segment.size == 0:
                        continue

                    allowed = before_cap - before_count
                    if allowed <= 0:
                        break

                    match_count = segment_filter_reversed(
                        segment.bundle,
                        effective_mask=self._effective_mask,
                        out_indices=indices.array,
                        max_matches=allowed,
                        kv=kv,
                        text=text,
                        **before_kwargs,
                    )

                    if match_count > 0:
                        consume_before(segment, indices.array, match_count)
                        oldest_seq = int(segment.bundle.sequences[indices.array[0]])
                        before_count += match_count

        # A single segment holding more than before_cap matching rows would otherwise read as
        # "reached the start" just because the cap happened to be hit right there - it hit quota
        # within that segment via max_matches, not because there's genuinely nothing older. Only
        # treat this as the true start when the fetch came back under quota; hitting quota
        # exactly always defers to the next backward page, which self-corrects (an empty
        # follow-up fetch then legitimately reports reached_start).
        reached_start = before_count < before_cap

        # --- "After" set: matches at/after the anchor, scanning oldest-to-newest ---
        after_count = 0
        newest_seq = anchor_seq if has_seq_anchor else None

        if has_seq_anchor:
            after_kwargs = {"start_seq": anchor_seq - 1}  # start_seq is an exclusive lower bound
        elif has_ts_anchor:
            after_kwargs = {"start_ts": anchor_ts}  # start_ts is an inclusive lower bound - no -1
        else:
            after_kwargs = {"start_seq": SEQ_NONE}

        with log_pool.get_snapshot() as segments, log_pool.acquire_indices_buffer() as indices:
            for segment in segments:
                if segment.size == 0:
                    continue

                allowed = after_cap - after_count
                if allowed <= 0:
                    break

                match_count = segment_filter(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    max_matches=allowed,
                    kv=kv,
                    text=text,
                    **after_kwargs,
                )

                if match_count > 0:
                    consume_after(segment, indices.array, match_count)
                    newest_seq = int(segment.bundle.sequences[indices.array[match_count - 1]])
                    after_count += match_count

        return HistoryScanResult(before_count, after_count, oldest_seq, newest_seq, reached_start)

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for the shared scan kernels (nb_filter_segment,
        nb_segment_filter_reversed) with every start_seq/end_seq/start_ts/end_ts shape scan_tail/
        scan_history_window actually use. Terminal per-consumer kernels (nb_segment_format,
        nb_segment_extract_fields) are warmed separately by LogTextFetcher.warmup/
        LogTableStore.warmup.

        kv/text are NamedTuples of numpy arrays, and Numba types an array's read-only-ness as
        part of its signature (see numba-njit skill Sec 3/9). EMPTY_KV_CONDITIONS/
        EMPTY_TEXT_SEARCH (ops/kv_filter.py, ops/text_filter.py) are deliberately built from
        np.frombuffer(b"", ...) rather than np.empty(...) so their buffer fields are already
        read-only - the same type Numba sees for a real, non-empty query. That means the single
        EMPTY_* combo exercised below covers the "real kv/text present" case too."""
        print("[Warmup] LogSegmentScanner ...")

        mod_count = helper.registry.module_count()
        safe_capacity = max(10, mod_count)
        effective_mask = np.full(safe_capacity, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
        effective_mask[helper.floats_mod.id] = LogLevel.ALL.value
        effective_mask[helper.warmup_mod.id] = LogLevel.ALL.value

        with helper.log_pool.get_reversed_snapshot() as segments, helper.log_pool.acquire_indices_buffer() as indices:
            for segment in segments:
                if segment.size == 0:
                    continue

                # scan_tail's shape: start_seq passed explicitly (no end_seq).
                segment_filter_reversed(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=1000,
                    start_seq=0,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )

                # scan_history_window's "before" shape: end_seq passed explicitly.
                segment_filter_reversed(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=1000,
                    end_seq=dtypes.SEQ_START,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )

                # scan_history_window's "after" shape: nb_filter_segment with start_seq bound.
                segment_filter(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=1000,
                    start_seq=0,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )
                break

        print("[Warmup] LogSegmentScanner ... done")


class LiveFetchResult(NamedTuple):
    text: str
    seqs: np.ndarray
    total_new_rows: int
    reached_live_edge: bool
    highest_seq_seen: int


class HistoryFetchResult(NamedTuple):
    before_count: int
    after_count: int
    oldest_seq: Optional[int]
    newest_seq: Optional[int]
    reached_start: bool
    text: str


class LogTextFetcher:
    """Thin text-output wrapper around LogSegmentScanner for LogViewerWidget/ConsoleSubscriber:
    owns the `consume` callback that formats each match to UTF-8 text via nb_segment_format, so
    neither text caller has to hand-write it. Also owns the live-tail watermark
    (`latest_seq_seen`) - a plain per-consumer counter, not scanner state (see
    LogSegmentScanner's docstring)."""

    def __init__(self, scanner: LogSegmentScanner, array_pool, tables_provider: Callable[[], object]):
        self._scanner = scanner
        self._array_pool = array_pool
        self._tables_provider = tables_provider
        self.latest_seq_seen = SEQ_NONE

    def invalidate_mask(self) -> None:
        self._scanner.invalidate_mask()

    def ensure_effective_mask(self) -> int:
        return self._scanner.ensure_effective_mask()

    def _make_format_consumer(self, format_cfg: FormattingConfig, tz_offset_sec: int, out_batches, out_seqs):
        array_pool = self._array_pool
        tables = self._tables_provider()

        def _consume(segment, indices_array, match_count):
            req_bytes = nb_segment_estimate_out_size(indices_array, match_count, segment.bundle, tables, format_cfg)
            with array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                bytes_written = nb_segment_format(
                    handle.array, indices_array, match_count, segment.bundle, tables, format_cfg, tz_offset_sec
                )
                out_batches.append(handle.array[:bytes_written].tobytes().decode("utf-8", errors="replace"))
            # indices_array is a pooled buffer reused next iteration - copy the matched rows'
            # sequence ids out now rather than keeping a view into it.
            out_seqs.append(segment.bundle.sequences[indices_array[:match_count]].copy())

        return _consume

    def fetch_live_tail(self, *, max_rows: int, format_cfg: FormattingConfig, tz_offset_sec: int) -> LiveFetchResult:
        batches: list = []
        seq_batches: list = []
        consume = self._make_format_consumer(format_cfg, tz_offset_sec, batches, seq_batches)

        result = self._scanner.scan_tail(start_seq=self.latest_seq_seen, max_rows=max_rows, consume=consume)
        self.latest_seq_seen = max(self.latest_seq_seen, result.highest_seq_seen)

        # Segments were processed newest-first, so reverse to yield chronological order.
        batches.reverse()
        seq_batches.reverse()
        seqs = np.concatenate(seq_batches) if seq_batches else np.empty(0, dtype=dtypes.SEQ_TYPE)

        return LiveFetchResult(
            "".join(batches), seqs, result.total_new_rows, result.reached_live_edge, result.highest_seq_seen
        )

    def fetch_history_window(
        self,
        *,
        anchor_seq: Optional[int] = None,
        anchor_ts: Optional[int] = None,
        before_cap: int,
        after_cap: int,
        format_cfg: FormattingConfig,
        tz_offset_sec: int,
    ) -> HistoryFetchResult:
        before_batches: list = []
        after_batches: list = []
        # Sequence tracking isn't needed by text callers here (oldest/newest seq is already
        # returned by the scan itself), so seq batches are thrown away for this consumer.
        consume_before = self._make_format_consumer(format_cfg, tz_offset_sec, before_batches, [])
        consume_after = self._make_format_consumer(format_cfg, tz_offset_sec, after_batches, [])

        result = self._scanner.scan_history_window(
            anchor_seq=anchor_seq,
            anchor_ts=anchor_ts,
            before_cap=before_cap,
            after_cap=after_cap,
            consume_before=consume_before,
            consume_after=consume_after,
        )

        # "Before" was scanned newest-to-oldest, so its batches need reversing to read
        # oldest-first; "after" was already scanned oldest-to-newest.
        before_batches.reverse()
        text_result = "".join(before_batches) + "".join(after_batches)

        return HistoryFetchResult(
            result.before_count,
            result.after_count,
            result.oldest_seq,
            result.newest_seq,
            result.reached_start,
            text_result,
        )

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Covers nb_segment_estimate_out_size/nb_segment_format - the terminal text-formatting
        kernels LogSegmentScanner.warmup doesn't exercise."""
        print("[Warmup] LogTextFetcher ...")

        mod_count = helper.registry.module_count()
        safe_capacity = max(10, mod_count)
        effective_mask = np.full(safe_capacity, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
        effective_mask[helper.floats_mod.id] = LogLevel.ALL.value
        effective_mask[helper.warmup_mod.id] = LogLevel.ALL.value

        format_cfg = FormattingConfig(True, True, True, True)

        with helper.log_pool.get_snapshot() as segments, helper.log_pool.acquire_indices_buffer() as indices:
            for segment in segments:
                match_count = segment_filter(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=1000,
                    start_seq=0,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )

                if match_count > 0:
                    req_bytes = nb_segment_estimate_out_size(
                        indices.array, match_count, segment.bundle, helper.registry.bundle(), format_cfg
                    )
                    with helper.array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                        nb_segment_format(
                            handle.array,
                            indices.array,
                            match_count,
                            segment.bundle,
                            helper.registry.bundle(),
                            format_cfg,
                            0,
                        )
                break

        print("[Warmup] LogTextFetcher ... done")
