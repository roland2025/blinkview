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
from blinkview.ops.segments import _nb_bundle_extend, _nb_bundle_push


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
        # Heterogeneous Extension Handles
        "_ext_u32_1_h",
        "_ext_u32_2_h",
        "_ext_u64_1_h",
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
        self._ext_u32_1_h = self._ext_u32_2_h = self._ext_u64_1_h = None
        self.bundle: Optional[LogBundle] = None

        self._allocate(
            req_capacity,
            buffer_bytes,
            has_levels,
            has_modules,
            has_devices,
            has_sequences,
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
            has_ext_u32_1=has_ext_u32_1,
            has_ext_u32_2=has_ext_u32_2,
            has_ext_u64_1=has_ext_u64_1,
        )

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
    ) -> bool:
        """
        Inserts data from any buffer-compatible object (bytes, bytearray, memoryview, ndarray).
        Uses np.frombuffer to create a zero-copy view for the Numba kernel.
        """
        if not (b := self.bundle):
            return False

        data_view = np.frombuffer(msg_data, dtype=dtypes.BYTE)

        return _nb_bundle_push(
            b, ts_ns, rx_ts_ns, data_view, level, module, device, seq, ext_u32_1, ext_u32_2, ext_u64_1
        )

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
    ) -> bool:
        """
        Inserts a new log record into the bundle via optimized Numba kernel.
        """
        if not (b := self.bundle):
            return False

        return _nb_bundle_push(
            b, ts_ns, rx_ts_ns, msg_bytes, level, module, device, seq, ext_u32_1, ext_u32_2, ext_u64_1
        )

    def append(self, msg_bytes: bytes) -> bool:
        if not (b := self.bundle):
            return False
        return _nb_bundle_extend(b, msg_bytes)

    def append_any(self, msg_data: Any) -> bool:
        if not (b := self.bundle):
            return False
        data_view = np.frombuffer(msg_data, dtype=dtypes.BYTE)
        return _nb_bundle_extend(b, data_view)

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

            # Extension release
            if self._ext_u32_1_h:
                self._ext_u32_1_h.release()
            if self._ext_u32_2_h:
                self._ext_u32_2_h.release()
            if self._ext_u64_1_h:
                self._ext_u64_1_h.release()

            self._ts_h = self._rx_ts_h = self._off_h = self._len_h = self._buf_h = None
            self._lvl_h = self._mod_h = self._dev_h = self._seq_h = None
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

    def __iter__(self):
        b = self.bundle
        if b is None:
            return

        # 1. Localize the count
        count = b.size[0]
        if count == 0:
            return

        # 2. Localize all Array references
        # Pulling these out of the 'b' object once
        timestamps = b.timestamps
        offsets = b.offsets
        lengths = b.lengths
        buffer = b.buffer

        levels = b.levels
        modules = b.modules
        devices = b.devices
        sequences = b.sequences

        ext_u32_1 = b.ext_u32_1
        ext_u32_2 = b.ext_u32_2
        ext_u64_1 = b.ext_u64_1

        # 3. Localize Flags
        has_lvls = b.has_levels
        has_mods = b.has_modules
        has_devs = b.has_devices
        has_seqs = b.has_sequences
        has_u32_1 = b.has_ext_u32_1
        has_u32_2 = b.has_ext_u32_2
        has_u64_1 = b.has_ext_u64_1

        # 4. High-speed Loop
        for i in range(count):
            off = offsets[i]
            # .tobytes() creates a copy; if you just need to read,
            # consider yielding a memoryview/slice instead.
            msg = buffer[off : off + lengths[i]]

            yield (
                timestamps[i],
                msg,
                levels[i] if has_lvls else None,
                modules[i] if has_mods else None,
                devices[i] if has_devs else None,
                sequences[i] if has_seqs else None,
                ext_u32_1[i] if has_u32_1 else None,
                ext_u32_2[i] if has_u32_2 else None,
                ext_u64_1[i] if has_u64_1 else None,
            )

    def iter_human(self):
        b = self.bundle
        if b is None:
            return

        # 1. Localize the count
        count = b.size[0]
        if count == 0:
            return

        # 2. Localize all Array references
        # Pulling these out of the 'b' object once
        timestamps = b.timestamps
        offsets = b.offsets
        lengths = b.lengths
        buffer = b.buffer

        levels = b.levels
        modules = b.modules
        devices = b.devices
        sequences = b.sequences

        ext_u32_1 = b.ext_u32_1
        ext_u32_2 = b.ext_u32_2
        ext_u64_1 = b.ext_u64_1

        # 3. Localize Flags
        has_lvls = b.has_levels
        has_mods = b.has_modules
        has_devs = b.has_devices
        has_seqs = b.has_sequences
        has_u32_1 = b.has_ext_u32_1
        has_u32_2 = b.has_ext_u32_2
        has_u64_1 = b.has_ext_u64_1

        # 4. High-speed Loop
        for i in range(count):
            off = offsets[i]
            # .tobytes() creates a copy; if you just need to read,
            # consider yielding a memoryview/slice instead.
            msg = buffer[off : off + lengths[i]].tobytes()

            yield (
                int(timestamps[i]),
                msg,
                int(levels[i]) if has_lvls else None,
                int(modules[i]) if has_mods else None,
                int(devices[i]) if has_devs else None,
                int(sequences[i]) if has_seqs else None,
                int(ext_u32_1[i]) if has_u32_1 else None,
                int(ext_u32_2[i]) if has_u32_2 else None,
                int(ext_u64_1[i]) if has_u64_1 else None,
            )

    def iter_time_messages(self):
        b = self.bundle
        if b is None:
            return

        for i in range(b.size[0]):
            offset = b.offsets[i]
            msg_bytes = b.buffer[offset : offset + b.lengths[i]].tobytes()
            yield b.timestamps[i], msg_bytes

    @property
    def start_ts(self) -> int:
        """
        Returns the timestamp of the first message.
        If empty, returns max int64 so time-delta checks safely fail.
        """
        # 9223372036854775807 is (2**63 - 1), the max for int64
        b = self.bundle
        return b.timestamps[0] if b is not None and b.size[0] > 0 else 9223372036854775807

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
        if (b := self.bundle) and (sz := b.size[0]) > 0:
            return b.sequences[sz - 1]
        return SEQ_NONE

    @property
    def first_sequence_id(self) -> dtypes.SEQ_TYPE:
        if (b := self.bundle) and b.size[0] > 0:
            return b.sequences[0]
        return SEQ_NONE
