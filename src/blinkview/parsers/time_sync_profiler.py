# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ops.segments import nb_find_next_module_index
from blinkview.ops.strings import nb_find_and_parse_int
from blinkview.parsers.parser import BaseParser, ParserFactory


def nb_bundle_parse_states(bundle, index, key_seq, key_target, key_pin):
    """
    Extracts seq, target_state, and pin_state in a single LLVM pass.
    """
    offset = bundle.offsets[index]
    length = bundle.lengths[index]
    buffer = bundle.buffer

    has_seq, seq = nb_find_and_parse_int(buffer, offset, length, key_seq)

    has_target, target = False, 0
    has_pin, pin = False, 0

    if has_seq:
        has_target, target = nb_find_and_parse_int(buffer, offset, length, key_target)
        # Optimization: if it's a target_state payload, it's unlikely to also have pin_state
        if not has_target:
            has_pin, pin = nb_find_and_parse_int(buffer, offset, length, key_pin)

    return has_seq, seq, has_target, target, has_pin, pin


# What a board logs when its edge counter is set (`sync seq <n>`): `new_seq_nr: <n>`
SEQ_RESET_PREFIX = b"new_seq_nr"

# An edge row waits this long (PC receive time) for its partner. Longer than any honest delay between the source's
# row and a consumer's (a Zephyr log thread may hold rows for a second), shorter than the gap between two runs, so
# that a row left unpaired by one run cannot pair with the next run's edge of the same seq.
PAIR_MAX_AGE_NS = 30_000_000_000
PAIR_MAX_ENTRIES = 8192


class SeqPairBook:
    """The boards' edge rows waiting for their partner, keyed by `seq`. Any board can be the source: the board whose
    `target_state` rows come in (`add_source`) is it, and every other board's `pin_state` rows (`add_consumer`)
    pair with those.

    Each entry is `(ts, state, rx_ts)`. An entry is dropped once it is PAIR_MAX_AGE_NS older (by receive time) than
    the newest row of its board, or when its board resets its counter (`reset`). A source row stays after pairing,
    as every consumer pairs with it; a consumer row is removed when it pairs. When another board starts logging
    source rows, the old source's rows and the new source's own waiting consumer rows are dropped: they are from the
    last run."""

    def __init__(self, board_ids, max_age_ns: int = PAIR_MAX_AGE_NS, max_entries: int = PAIR_MAX_ENTRIES):
        self.source_id: int | None = None
        self.source: dict[int, tuple[int, int, int]] = {}
        self.consumers: dict[int, dict[int, tuple[int, int, int]]] = {dev: {} for dev in board_ids}
        self.max_age_ns = max_age_ns
        self.max_entries = max_entries

    def reset(self, dev_id):
        """`dev_id` restarted its edge counter (`new_seq_nr`): its waiting rows are from the last run."""
        self.consumers[dev_id].clear()
        if dev_id == self.source_id:
            self.source.clear()

    def _evict(self, book: dict, rx_ts: int):
        cutoff = rx_ts - self.max_age_ns
        while book:
            oldest_seq = next(iter(book))
            if book[oldest_seq][2] >= cutoff and len(book) <= self.max_entries:
                break
            del book[oldest_seq]

    def add_source(self, source_id, seq, ts, state, rx_ts):
        """Stores an edge of board `source_id` as source; returns `(consumer_id, (ts, state), (ts, state))` for each
        consumer row that was waiting for it (source first)."""
        if source_id != self.source_id:
            self.source.clear()
            self.consumers[source_id].clear()
            self.source_id = source_id
        book = self.source
        book.pop(seq, None)  # re-insert at the end: eviction goes by insertion order
        book[seq] = (ts, state, rx_ts)
        self._evict(book, rx_ts)
        matches = []
        for dev_id, waiting in self.consumers.items():
            if dev_id == source_id:
                continue
            self._evict(waiting, rx_ts)
            entry = waiting.pop(seq, None)
            if entry is not None:
                matches.append((dev_id, (ts, state), (entry[0], entry[1])))
        return matches

    def add_consumer(self, dev_id, seq, ts, state, rx_ts):
        """Stores a consumer edge, or pairs it at once with the source's row of that seq; returns
        `(consumer_id, (ts, state), (ts, state))` for the pair made (source first), else None. A second row of the
        same seq while the first one waits is ignored. The source's own consumer rows (it was switched to consumer
        for the next run) wait for the next source."""
        self._evict(self.source, rx_ts)
        waiting = self.consumers[dev_id]
        self._evict(waiting, rx_ts)
        src = self.source.get(seq) if dev_id != self.source_id else None
        if src is not None:
            first = waiting.pop(seq, None)
            pin = (first[0], first[1]) if first is not None else (ts, state)
            return dev_id, (src[0], src[1]), pin
        if seq not in waiting:
            waiting[seq] = (ts, state, rx_ts)
        return None


@ParserFactory.register("time_sync_profiler")
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/pipelines"},
)
@configuration_property(
    "server",
    type="string",
    required=False,
    _reference="/pipelines",
    default="",
    description="One more board to pair, besides the sources. Any board can be the source: the one that logs "
    "`target_state` rows is it.",
)
@configuration_property(
    "time_source_boot",
    type="boolean",
    required=True,
    ui_order=40,
)
class TimeSyncProfiler(BaseParser):
    def __init__(self):
        super().__init__()

    def run(self):
        # --- Localize Framework Plumbing ---
        logger = self.logger
        _get = self.input_queue.get
        _time_ns = self.shared.time_ns
        _distribute = self.distribute
        registry = self.shared.registry
        pool_create = self.shared.array_pool.create

        # Base timeout configuration (strictly capped at 50ms)
        requested_delay = getattr(self, "delay", 50)  # Default to 50ms if unset
        actual_delay = min(requested_delay, 50)  # Enforce the 50ms ceiling

        max_timeout = actual_delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)

        # --- Boards ---
        # Every board can be the source of a run; the profiler only listens. It used to make `server` the source
        # and send `sync seq 0` to all on every (re)start, which fought the line when another board was the source.
        board_refs = list(self.sources_)
        if self.server and self.server not in board_refs:
            board_refs.append(self.server)
        board_devices = [registry.get_reference_target(ref).local.device_id for ref in board_refs]
        board_names = {dev.id: str(dev.name) for dev in board_devices}
        logger.info("boards: %s", board_devices)

        # --- Cache Modules ---
        main_module_ids = {}
        for dev in board_devices:
            try:
                main_module_ids[dev.id] = dev.get_module("main").id
            except Exception as e:
                logger.warning("Could not cache 'main' module for device %s: %s", dev.id, e)

        local_device_id_int = self.local.device_id.id

        # The module ID to tag our outgoing metric batches with: one per board, the board's offset from the source
        out_module_ids = {}
        for dev in board_devices:
            board_name = str(dev.name)
            try:
                out_module_ids[dev.id] = self.local.device_id.get_module(board_name).id
            except Exception as e:
                logger.warning("Could not cache output module '%s' on local device: %s", board_name, e)

        # --- Output Batch Management ---
        def batch_acquire():
            return pool_create(
                PooledLogBatch,
                1024,  # capacity
                4096,  # buffer limit
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        batch_out = batch_acquire()
        batch_out_time = _time_ns()
        batch_out_capacity = batch_out.capacity
        batch_out_buf_limit = batch_out.buffer_capacity() * 0.9

        def flush():
            nonlocal batch_out, batch_out_time, batch_out_capacity, batch_out_buf_limit
            if batch_out and batch_out.size > 0:
                with batch_out:
                    _distribute(batch_out)
            batch_out = batch_acquire()
            batch_out_time = _time_ns()
            batch_out_capacity = batch_out.capacity
            batch_out_buf_limit = batch_out.buffer_capacity() * 0.9

        book = SeqPairBook(main_module_ids)
        reset_prefix_len = len(SEQ_RESET_PREFIX)

        def emit(pair, seq, rx_ts, level):
            dev_id, (t_target, target_val), (t_pin, pin_val) = pair
            if target_val != pin_val:
                logger.warning(
                    "Desync detected on device %s at seq %s! Expected target state %s, got pin state %s.",
                    dev_id,
                    seq,
                    target_val,
                    pin_val,
                )
                return
            if dev_id not in out_module_ids:
                return
            diff_ms = (t_pin - t_target) / 1_000_000.0
            try:
                batch_out.insert(
                    ts_ns=t_target,
                    rx_ts_ns=rx_ts,
                    msg_bytes=f"{diff_ms:.3f}".encode("ascii"),
                    level=level,
                    module=out_module_ids[dev_id],
                    device=local_device_id_int,
                )
            except Exception:
                pass

        stop_is_set = self._stop_event.is_set

        # Pre-allocate Byte Keys for Numba Zero-Allocation Lookups
        KEY_SEQ = np.frombuffer(b"seq", dtype=np.uint8)
        KEY_TARGET_STATE = np.frombuffer(b"target_state", dtype=np.uint8)
        KEY_PIN_STATE = np.frombuffer(b"pin_state", dtype=np.uint8)

        # --- Hot Loop ---
        while not stop_is_set():
            now = _time_ns()

            # --- Dynamic Polling Timeout ---
            if batch_out.size > 0:
                elapsed_ns = now - batch_out_time
                current_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
            else:
                current_timeout = max_timeout

            # --- 2. Process Incoming Rx Batches ---
            batch_in: PooledLogBatch = _get(timeout=current_timeout)

            if batch_in is None:
                if batch_out.size > 0:
                    flush()
                continue

            with batch_in:
                b = batch_in.bundle

                if batch_out.size == 0:
                    batch_out_time = _time_ns()

                device_id_int = batch_in.get_device()

                if device_id_int not in main_module_ids:
                    continue

                main_module_id = main_module_ids[device_id_int]
                cursor = 0

                # Memoryviews for fast 1D array indexing in pure Python
                timestamps = memoryview(b.timestamps)
                rx_timestamps = memoryview(b.rx_timestamps)
                levels = memoryview(b.levels)

                try:
                    while True:
                        found, idx = nb_find_next_module_index(
                            b, dtypes.ID_TYPE(main_module_id), dtypes.SEQ_TYPE(cursor)
                        )

                        if not found:
                            break

                        # Update: Capture target_val and pin_val instead of discarding them
                        has_seq, seq, has_target, target_val, has_pin, pin_val = nb_bundle_parse_states(
                            b, idx, KEY_SEQ, KEY_TARGET_STATE, KEY_PIN_STATE
                        )

                        if has_seq:
                            ts = timestamps[idx]
                            rx_ts = rx_timestamps[idx]

                            if has_target:
                                if device_id_int != book.source_id:
                                    logger.info("source: %s", board_names[device_id_int])
                                for pair in book.add_source(device_id_int, seq, ts, target_val, rx_ts):
                                    emit(pair, seq, rx_ts, levels[idx])

                            elif has_pin:
                                pair = book.add_consumer(device_id_int, seq, ts, pin_val, rx_ts)
                                if pair is not None:
                                    emit(pair, seq, rx_ts, levels[idx])

                        elif b.lengths[idx] >= reset_prefix_len:
                            # `sync seq <n>` restarts the board's counter: its waiting rows are from the last run
                            msg_start = int(b.offsets[idx])
                            if b.buffer[msg_start : msg_start + reset_prefix_len].tobytes() == SEQ_RESET_PREFIX:
                                book.reset(device_id_int)

                        cursor = idx + 1
                finally:
                    timestamps.release()
                    rx_timestamps.release()
                    levels.release()
                    timestamps = rx_timestamps = levels = None
            # Flush by age
            if batch_out.size > 0 and (_time_ns() - batch_out_time >= max_timeout_ns):
                flush()

        # --- Drain on Shutdown ---
        if batch_out:
            if batch_out.size > 0:
                flush()
            else:
                batch_out.release()
