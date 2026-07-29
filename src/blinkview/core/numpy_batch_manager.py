# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from typing import Any, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.types.empty import EMPTY_ID, EMPTY_LEVEL, EMPTY_SEQ
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.segments import nb_bundle_extend, nb_bundle_push, nb_bundle_push_len


class PooledLogBatch:
    """
    Unified high-performance transport and storage object for logs.
    Handles both transient log batches and long-term circular buffer segments.
    """

    __slots__ = (
        "metadata",  # Flexible slot for segment_seq or other IDs
        "bundle",  # The unified LogBundle source of truth
        "_pool",
        "_ref_count",
        "_lock",
        "in_use",
        # Memory handles for the central pool
        "_ts_h",
        "_rx_ts_h",
        "_off_h",
        "_len_h",
        "_buf_h",
        "_lvl_h",
        "_mod_h",
        "_dev_h",
        "_seq_h",
        "_pid_h",
        "_tid_h",
        # Heterogeneous Extension Handles
        "_ext_u32_1_h",
        "_ext_u32_2_h",
        "_ext_u64_1_h",
        # Plain-int cache of first/last row seq+ts, so start_ts/end_ts/first_sequence_id/
        # last_sequence_id never have to index into the (possibly mmap'd) bundle arrays - see
        # plans/fetch-telemetry-window-cold-segment-perf.md. Uniform for hot and cold segments:
        # hot segments update it incrementally at insert time (_note_inserted_row/
        # note_appended_rows); cold (memmap) segments populate it once, up front, straight from
        # the on-disk header in from_memmap - no per-read branching on segment type needed either
        # way.
        "_cached_first_seq",
        "_cached_first_ts",
        "_cached_last_seq",
        "_cached_last_ts",
    )

    def __init__(
        self,
        pool: Any,
        req_capacity: int,
        buffer_bytes: int,
        has_levels: bool = False,
        has_modules: bool = False,
        has_devices: bool = False,
        has_sequences: bool = False,
        has_pids: bool = False,
        has_tids: bool = False,
        has_ext_u32_1: bool = False,
        has_ext_u32_2: bool = False,
        has_ext_u64_1: bool = False,
        metadata: Any = None,
    ):
        self._pool = pool
        self.metadata = metadata
        self._ref_count = 1
        self._lock = Lock()
        self.in_use = True

        self._ts_h = self._rx_ts_h = self._off_h = self._len_h = self._buf_h = None
        self._lvl_h = self._mod_h = self._dev_h = self._seq_h = None
        self._pid_h = self._tid_h = None
        self._ext_u32_1_h = self._ext_u32_2_h = self._ext_u64_1_h = None
        self.bundle: Optional[LogBundle] = None

        self._cached_first_seq = None
        self._cached_first_ts = None
        self._cached_last_seq = SEQ_NONE
        self._cached_last_ts = None

        self._allocate(
            req_capacity,
            buffer_bytes,
            has_levels,
            has_modules,
            has_devices,
            has_sequences,
            has_pids,
            has_tids,
            has_ext_u32_1,
            has_ext_u32_2,
            has_ext_u64_1,
        )

    def _allocate(
        self,
        req_capacity,
        buffer_bytes,
        has_levels,
        has_modules,
        has_devices,
        has_sequences,
        has_pids,
        has_tids,
        has_ext_u32_1,
        has_ext_u32_2,
        has_ext_u64_1,
    ):
        acquire = self._pool.acquire

        # 1. Mandatory Columns
        self._ts_h = acquire(req_capacity, dtype=dtypes.TS_TYPE)
        ts_arr = self._ts_h.array
        true_cap = len(ts_arr)

        self._rx_ts_h = acquire(req_capacity, dtype=dtypes.TS_TYPE)

        self._off_h = acquire(true_cap, dtype=dtypes.OFFSET_TYPE)
        self._len_h = acquire(true_cap, dtype=dtypes.LEN_TYPE)
        self._buf_h = acquire(buffer_bytes, dtype=dtypes.BYTE)

        # 2. Optional Columns - Consistent handle storage
        if has_levels:
            self._lvl_h = acquire(true_cap, dtype=dtypes.LEVEL_TYPE)
            lvl_arr = self._lvl_h.array
        else:
            lvl_arr = EMPTY_LEVEL

        if has_modules:
            self._mod_h = acquire(true_cap, dtype=dtypes.ID_TYPE)
            mod_arr = self._mod_h.array
        else:
            mod_arr = EMPTY_ID

        if has_devices:
            self._dev_h = acquire(true_cap, dtype=dtypes.ID_TYPE)
            dev_arr = self._dev_h.array
        else:
            dev_arr = EMPTY_ID

        if has_sequences:
            self._seq_h = acquire(true_cap, dtype=dtypes.SEQ_TYPE)
            seq_arr = self._seq_h.array
        else:
            seq_arr = EMPTY_SEQ

        if has_pids:
            self._pid_h = acquire(true_cap, dtype=dtypes.ID_TYPE)
            pid_arr = self._pid_h.array
        else:
            pid_arr = EMPTY_ID

        if has_tids:
            self._tid_h = acquire(true_cap, dtype=dtypes.ID_TYPE)
            tid_arr = self._tid_h.array
        else:
            tid_arr = EMPTY_ID

        # 3. Independent Heterogeneous Extension Columns
        if has_ext_u32_1:
            self._ext_u32_1_h = acquire(true_cap, dtype=dtypes.UINT32)
            arr_u32_1 = self._ext_u32_1_h.array
        else:
            arr_u32_1 = np.empty(0, dtype=np.uint32)

        if has_ext_u32_2:
            self._ext_u32_2_h = acquire(true_cap, dtype=dtypes.UINT32)
            arr_u32_2 = self._ext_u32_2_h.array
        else:
            arr_u32_2 = np.empty(0, dtype=np.uint32)

        if has_ext_u64_1:
            self._ext_u64_1_h = acquire(true_cap, dtype=dtypes.UINT64)
            arr_u64_1 = self._ext_u64_1_h.array
        else:
            arr_u64_1 = np.empty(0, dtype=np.uint64)

        # 4. Counters & Baking
        self.bundle = LogBundle(
            timestamps=ts_arr,
            rx_timestamps=self._rx_ts_h.array,
            levels=lvl_arr,
            modules=mod_arr,
            devices=dev_arr,
            sequences=seq_arr,
            pids=pid_arr,
            tids=tid_arr,
            offsets=self._off_h.array,
            lengths=self._len_h.array,
            buffer=self._buf_h.array,
            # Extensions
            ext_u32_1=arr_u32_1,
            ext_u32_2=arr_u32_2,
            ext_u64_1=arr_u64_1,
            # Metadata
            size=np.zeros(1, dtype=np.int64),
            msg_cursor=np.zeros(1, dtype=np.int64),
            capacity=true_cap,
            has_levels=has_levels,
            has_modules=has_modules,
            has_devices=has_devices,
            has_sequences=has_sequences,
            has_pids=has_pids,
            has_tids=has_tids,
            has_ext_u32_1=has_ext_u32_1,
            has_ext_u32_2=has_ext_u32_2,
            has_ext_u64_1=has_ext_u64_1,
        )

    @classmethod
    def from_memmap(cls, path: Any, metadata: Any = None) -> "PooledLogBatch":
        """Alternate constructor: builds a frozen, read-only PooledLogBatch backed by a
        memory-mapped cold-segment file (core/cold_segment.py) instead of NumpyArrayPool slabs.
        Every other method on this class (release(), retain(), __iter__, __getitem__, start_ts,
        last_sequence_id, ...) works unmodified against the result - the only difference from a
        normally-constructed instance is where the column arrays physically live, which is
        exactly the point (Numba kernels accept np.memmap/np.frombuffer views identically to pool
        arrays). The arrays are read-only, matching a cold segment's frozen-forever contract -
        insert()/append() on the result will raise rather than silently corrupt anything."""
        from blinkview.core.cold_segment import open_cold_segment_arrays

        self = object.__new__(cls)
        self._pool = None
        self.metadata = metadata
        self._ref_count = 1
        self._lock = Lock()
        self.in_use = True

        header, handles = open_cold_segment_arrays(path)

        self._ts_h = handles["timestamps"]
        self._rx_ts_h = handles["rx_timestamps"]
        self._off_h = handles["offsets"]
        self._len_h = handles["lengths"]
        self._buf_h = handles["buffer"]
        self._lvl_h = handles["levels"]
        self._mod_h = handles["modules"]
        self._dev_h = handles["devices"]
        self._seq_h = handles["sequences"]
        self._pid_h = handles["pids"]
        self._tid_h = handles["tids"]
        self._ext_u32_1_h = self._ext_u32_2_h = self._ext_u64_1_h = None

        # Populated once, up front, straight from the on-disk header - a cold segment never
        # changes again, so unlike a hot segment there's no later insert()/note_appended_rows()
        # call to keep this in sync with; it's simply set correctly from the start.
        if header.row_count > 0:
            self._cached_first_seq = header.first_seq
            self._cached_first_ts = header.earliest_ts
            self._cached_last_seq = header.last_seq
            self._cached_last_ts = header.latest_ts
        else:
            self._cached_first_seq = None
            self._cached_first_ts = None
            self._cached_last_seq = SEQ_NONE
            self._cached_last_ts = None

        self.bundle = LogBundle(
            timestamps=self._ts_h.array,
            rx_timestamps=self._rx_ts_h.array,
            levels=self._lvl_h.array,
            modules=self._mod_h.array,
            devices=self._dev_h.array,
            sequences=self._seq_h.array,
            pids=self._pid_h.array,
            tids=self._tid_h.array,
            offsets=self._off_h.array,
            lengths=self._len_h.array,
            buffer=self._buf_h.array,
            ext_u32_1=np.empty(0, dtype=np.uint32),
            ext_u32_2=np.empty(0, dtype=np.uint32),
            ext_u64_1=np.empty(0, dtype=np.uint64),
            size=np.array([header.row_count], dtype=np.int64),
            msg_cursor=np.array([header.buffer_len], dtype=np.int64),
            capacity=header.row_count,
            has_levels=True,
            has_modules=True,
            has_devices=True,
            has_sequences=True,
            has_pids=True,
            has_tids=True,
            has_ext_u32_1=False,
            has_ext_u32_2=False,
            has_ext_u64_1=False,
        )

        return self

    @property
    def size(self) -> int:
        return int(b.size[0]) if (b := self.bundle) else 0

    @property
    def capacity(self) -> int:
        return b.capacity if (b := self.bundle) else 0

    def clear(self, new_metadata: Any = None):
        """O(1) reset of counters and optional metadata update."""
        if b := self.bundle:
            b.size[0] = 0
            b.msg_cursor[0] = 0
        if new_metadata is not None:
            self.metadata = new_metadata
        self._cached_first_seq = None
        self._cached_first_ts = None
        self._cached_last_seq = SEQ_NONE
        self._cached_last_ts = None

    @property
    def msg_cursor(self) -> int:
        return int(b.msg_cursor[0]) if (b := self.bundle) else 0

    def insert_any(
        self,
        ts_ns: int,
        rx_ts_ns: int,
        msg_data: Any,
        level: int = 0,
        module: int = 0,
        device: int = 0,
        seq: int = 0,
        ext_u32_1: int = 0,
        ext_u32_2: int = 0,
        ext_u64_1: int = 0,
        pid: int = 0,
        tid: int = 0,
    ) -> bool:
        """
        Inserts data from any buffer-compatible object (bytes, bytearray, memoryview, ndarray).
        Uses np.frombuffer to create a zero-copy view for the Numba kernel.
        """
        if not (b := self.bundle):
            return False

        data_view = np.frombuffer(msg_data, dtype=dtypes.BYTE)

        success = nb_bundle_push(
            b, ts_ns, rx_ts_ns, data_view, level, module, device, seq, ext_u32_1, ext_u32_2, ext_u64_1, pid, tid
        )
        if success:
            self._note_inserted_row(ts_ns, seq)
        return success

    def insert_view(
        self,
        ts_ns: int,
        rx_ts_ns: int,
        data_view: np.ndarray,
        msg_length: int,
        level: int = 0,
        module: int = 0,
        device: int = 0,
        seq: int = 0,
        ext_u32_1: int = 0,
        ext_u32_2: int = 0,
        ext_u64_1: int = 0,
        pid: int = 0,
        tid: int = 0,
    ) -> bool:
        """
        Directly passes a pre-allocated zero-copy NumPy view to the Numba kernel.
        Achieves absolute minimum Python overhead.
        """
        if not (b := self.bundle):
            return False

        success = nb_bundle_push_len(
            b,
            ts_ns,
            rx_ts_ns,
            data_view,
            msg_length,
            level,
            module,
            device,
            seq,
            ext_u32_1,
            ext_u32_2,
            ext_u64_1,
            pid,
            tid,
        )
        if success:
            self._note_inserted_row(ts_ns, seq)
        return success

    def insert(
        self,
        ts_ns: int,
        rx_ts_ns: int,
        msg_bytes: bytes,
        level: int = 0,
        module: int = 0,
        device: int = 0,
        seq: int = 0,
        ext_u32_1: int = 0,
        ext_u32_2: int = 0,
        ext_u64_1: int = 0,
        pid: int = 0,
        tid: int = 0,
    ) -> bool:
        """
        Inserts a new log record into the bundle via optimized Numba kernel.
        """
        if not (b := self.bundle):
            return False

        # print(
        #     f"PooledLogBatch insert -> "
        #     f"self: {type(self).__name__}, "
        #     f"ts_ns: {type(ts_ns).__name__}, "
        #     f"rx_ts_ns: {type(rx_ts_ns).__name__}, "
        #     f"msg_bytes: {type(msg_bytes).__name__}, "
        #     f"level: {type(level).__name__}, "
        #     f"module: {type(module).__name__}, "
        #     f"device: {type(device).__name__}, "
        #     f"seq: {type(seq).__name__}, "
        #     f"ext_u32_1: {type(ext_u32_1).__name__}, "
        #     f"ext_u32_2: {type(ext_u32_2).__name__}, "
        #     f"ext_u64_1: {type(ext_u64_1).__name__}, "
        # )

        success = nb_bundle_push(
            b, ts_ns, rx_ts_ns, msg_bytes, level, module, device, seq, ext_u32_1, ext_u32_2, ext_u64_1, pid, tid
        )
        if success:
            self._note_inserted_row(ts_ns, seq)
        return success

    def _note_inserted_row(self, ts_ns: int, seq) -> None:
        """Updates the plain-int first/last seq+ts cache after a single successful insert -
        called with the exact values just inserted, so no array read-back is needed here."""
        if self._cached_first_seq is None:
            self._cached_first_seq = seq
            self._cached_first_ts = ts_ns
        self._cached_last_seq = seq
        self._cached_last_ts = ts_ns

    def note_appended_rows(self, new_row_count: int) -> None:
        """Counterpart to _note_inserted_row for writers that append rows directly into
        self.bundle's arrays via a Numba kernel rather than going through insert()/insert_any()
        (CircularLogPool.batch_append's nb_copy_batch_to_segment is the main one - by far the
        highest-throughput path real ingested data takes). Reads back just the newly-written
        tail of the arrays - cheap here since those exact locations were just written and are
        still hot in cache, unlike re-deriving them on every later read, which is the cost this
        cache exists to avoid. Must be called with the pool's own lock held (matching the
        Numba write it's following) so this read observes a fully-written row, not a torn one."""
        if new_row_count <= 0:
            return
        b = self.bundle
        if b is None:
            return
        size = int(b.size[0])
        last_idx = size - 1
        self._cached_last_seq = b.sequences[last_idx]
        self._cached_last_ts = b.timestamps[last_idx]
        if self._cached_first_seq is None:
            first_idx = size - new_row_count
            self._cached_first_seq = b.sequences[first_idx]
            self._cached_first_ts = b.timestamps[first_idx]

    def append(self, msg_bytes: bytes) -> bool:
        if not (b := self.bundle):
            return False
        return nb_bundle_extend(b, msg_bytes)

    def append_any(self, msg_data: Any) -> bool:
        if not (b := self.bundle):
            return False
        data_view = np.frombuffer(msg_data, dtype=dtypes.BYTE)
        return nb_bundle_extend(b, data_view)

    def retain(self):
        with self._lock:
            if self._ref_count <= 0:
                raise RuntimeError("Cannot retain a released batch.")
            self._ref_count += 1
        return self

    def release(self):
        with self._lock:
            self._ref_count -= 1
            if self._ref_count > 0:
                return
            self.clear()
            self.in_use = False
            self.bundle = None

            if self._ts_h:
                self._ts_h.release()

            if self._rx_ts_h:
                self._rx_ts_h.release()
            if self._off_h:
                self._off_h.release()
            if self._len_h:
                self._len_h.release()
            if self._buf_h:
                self._buf_h.release()
            if self._lvl_h:
                self._lvl_h.release()
            if self._mod_h:
                self._mod_h.release()
            if self._dev_h:
                self._dev_h.release()
            if self._seq_h:
                self._seq_h.release()
            if self._pid_h:
                self._pid_h.release()
            if self._tid_h:
                self._tid_h.release()

            # Extension release
            if self._ext_u32_1_h:
                self._ext_u32_1_h.release()
            if self._ext_u32_2_h:
                self._ext_u32_2_h.release()
            if self._ext_u64_1_h:
                self._ext_u64_1_h.release()

            self._ts_h = self._rx_ts_h = self._off_h = self._len_h = self._buf_h = None
            self._lvl_h = self._mod_h = self._dev_h = self._seq_h = None
            self._pid_h = self._tid_h = None
            self._ext_u32_1_h = self._ext_u32_2_h = self._ext_u64_1_h = None

    def __len__(self):
        return self.size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def buffer_capacity(self):
        with self._lock:
            b = self.bundle
            return len(b.buffer) if b is not None and b.buffer is not None else 0

    def __repr__(self):
        with self._lock:
            b = self.bundle
            if b is None:
                return f"PooledLogBatch(id={id(self):x} state=released)"

            buffer_len = len(b.buffer) if b.buffer is not None else 0
            return f"PooledLogBatch(id={id(self):x} size={b.size[0]} capacity={b.capacity}, buffer_used={b.msg_cursor[0]}/{buffer_len}, ref={self._ref_count})"

    __str__ = __repr__

    def __iter__(self, native=True):
        b = self.bundle
        if b is None:
            return

        # 1. Localize the count
        count = b.size[0]
        if count == 0:
            return

        # 1. Cast core mandatory boundaries to memoryviews
        timestamps = memoryview(b.timestamps)
        rx_timestamps = memoryview(b.rx_timestamps)
        offsets = memoryview(b.offsets)
        lengths = memoryview(b.lengths)
        buffer = memoryview(b.buffer)

        # Localize Flags
        has_levels = b.has_levels
        has_modules = b.has_modules
        has_devices = b.has_devices
        has_sequences = b.has_sequences
        has_ext_u32_1 = b.has_ext_u32_1
        has_ext_u32_2 = b.has_ext_u32_2
        has_ext_u64_1 = b.has_ext_u64_1

        has_pids = b.has_pids
        has_tids = b.has_tids

        # Conditional memoryview allocation (Only touch active metadata arrays)
        levels = memoryview(b.levels) if has_levels else None
        modules = memoryview(b.modules) if has_modules else None
        devices = memoryview(b.devices) if has_devices else None
        sequences = memoryview(b.sequences) if has_sequences else None

        ext_u32_1 = memoryview(b.ext_u32_1) if has_ext_u32_1 else None
        ext_u32_2 = memoryview(b.ext_u32_2) if has_ext_u32_2 else None
        ext_u64_1 = memoryview(b.ext_u64_1) if has_ext_u64_1 else None

        pids = memoryview(b.pids) if has_pids else None
        tids = memoryview(b.tids) if has_tids else None

        for i in range(count):
            off = offsets[i]

            yield (
                timestamps[i],
                buffer[off : off + lengths[i]].tobytes() if native else buffer[off : off + lengths[i]],
                rx_timestamps[i],
                levels[i] if has_levels else None,
                modules[i] if has_modules else None,
                devices[i] if has_devices else None,
                sequences[i] if has_sequences else None,
                ext_u32_1[i] if has_ext_u32_1 else None,
                ext_u32_2[i] if has_ext_u32_2 else None,
                ext_u64_1[i] if has_ext_u64_1 else None,
                pids[i] if has_pids else None,
                tids[i] if has_tids else None,
            )

    @property
    def start_ts(self) -> int:
        """
        Returns the timestamp of the first message, from the plain-int cache maintained by
        insert()/note_appended_rows() (hot segments) or populated once up front from the on-disk
        header (from_memmap/cold segments) - never by indexing into b.timestamps[0], since that's
        a numpy scalar array read on what's otherwise a cheap, frequently-polled property (see
        plans/fetch-telemetry-window-cold-segment-perf.md).
        If empty, returns max int64 so time-delta checks safely fail.
        """
        # 9223372036854775807 is (2**63 - 1), the max for int64
        return self._cached_first_ts if self._cached_first_ts is not None else 9223372036854775807

    @property
    def end_ts(self) -> int:
        """Counterpart to start_ts: the timestamp of the last message. If empty, returns min
        int64 so time-delta/range checks safely fail on the other side."""
        # -9223372036854775808 is -(2**63), the min for int64
        return self._cached_last_ts if self._cached_last_ts is not None else -9223372036854775808

    def __getitem__(self, index):
        """
        Allows indexed access to log rows.
        Returns the same tuple format as __iter__.
        """
        b = self.bundle
        if b is None:
            raise RuntimeError("Cannot access elements of a released batch.")

        current_size = b.size[0]

        # 1. Handle Slicing (e.g., batch[1:5])
        if isinstance(index, slice):
            indices = range(*index.indices(current_size))
            return [self[i] for i in indices]

        # 2. Handle Integer Indexing
        if not isinstance(index, int):
            raise TypeError(f"Index must be an integer or slice, not {type(index).__name__}")

        # Support negative indexing (e.g., -1 for the last row)
        if index < 0:
            index += current_size

        if index < 0 or index >= current_size:
            raise IndexError("PooledLogBatch index out of range")

        # 3. Extract Row Data (SoA to AoS conversion)
        offset = b.offsets[index]
        length = b.lengths[index]
        msg_bytes = b.buffer[offset : offset + length].tobytes()

        return (
            b.timestamps[index],
            msg_bytes,
            b.levels[index] if b.has_levels else None,
            b.modules[index] if b.has_modules else None,
            b.devices[index] if b.has_devices else None,
            b.sequences[index] if b.has_sequences else None,
            b.ext_u32_1[index] if b.has_ext_u32_1 else None,
            b.ext_u32_2[index] if b.has_ext_u32_2 else None,
            b.ext_u64_1[index] if b.has_ext_u64_1 else None,
        )

    def get_device(self) -> int:
        """
        Returns the device ID of the first message in the batch.
        Defaults to 0 if the batch is empty or device tracking is disabled.
        """
        b = self.bundle
        # Note: b.size[0] is used because 'size' is a 1-element numpy array
        if b and b.has_devices and b.size[0] > 0:
            return int(b.devices[0])
        return 0

    @property
    def last_sequence_id(self) -> dtypes.SEQ_TYPE:
        """From the same plain-int cache as start_ts, instead of b.sequences[sz - 1]'s numpy
        scalar array index. This property backs the early-break check in every reverse segment
        scan (scan_tail, scan_history_window, build_snapshot_as_of, ...), so it's polled once per
        segment visited by every single one of those scans (see
        plans/fetch-telemetry-window-cold-segment-perf.md)."""
        return self._cached_last_seq

    @property
    def first_sequence_id(self) -> dtypes.SEQ_TYPE:
        """See last_sequence_id's docstring - same cached shortcut."""
        return self._cached_first_seq if self._cached_first_seq is not None else SEQ_NONE


lock_log_batch = Lock()


def log_batch(instance, batch_data, direction="OUT"):
    """
    Logs batch data for both incoming and outgoing streams.

    :param instance: The object instance (usually 'self') to get the class name.
    :param batch_data: An iterable of tuples containing (_ts, _msg, _rx_ts, *rest).
    :param direction: String indicating traffic direction, e.g., "OUT" or "IN".
    """
    cls_name = instance.__class__.__name__
    direction = direction.upper()

    with lock_log_batch:
        # Dynamic header line
        print(f"[{cls_name}_{direction}] {batch_data}")

        # Dynamic detail lines
        for item in batch_data:
            _ts, _msg, _rx_ts, pid, tid = item[0], item[1], item[2], item[10], item[11]
            print(f"[{cls_name}_{direction}] ts={_ts} rx_ts={_rx_ts} pid={pid} tid={tid} msg={_msg} hex={_msg.hex()}")
