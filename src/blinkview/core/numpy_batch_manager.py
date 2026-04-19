# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from typing import NamedTuple

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle


class PooledLogBatch:
    """
    A high-performance, slotted transport object for logs.
    Dynamically acquires power-of-two columnar arrays from a central NumpyArrayPool.
    """

    EMPTY_U8 = np.empty(0, dtype=dtypes.BYTE)
    EMPTY_U16 = np.empty(0, dtype=dtypes.ID_TYPE)
    EMPTY_U64 = np.empty(0, dtype=dtypes.SEQ_TYPE)

    __slots__ = (
        "capacity",
        "_size_arr",
        "_cursor_arr",
        "_pool",
        "_ref_count",
        "_lock",
        "in_use",
        "has_levels",
        "has_modules",
        "has_devices",
        "has_sequences",
        # Un-sliced raw slabs
        "timestamps",
        "offsets",
        "lengths",
        "buffer",
        "levels",
        "modules",
        "devices",
        "sequences",
        # Internal handles to return memory to the pool
        "_ts_h",
        "_off_h",
        "_len_h",
        "_buf_h",
        "_lvl_h",
        "_mod_h",
        "_dev_h",
        "_seq_h",
    )

    def __init__(
        self,
        pool,
        req_capacity: int,
        req_buffer_kb: int,
        has_levels: bool = False,
        has_modules: bool = False,
        has_devices: bool = False,
        has_sequences: bool = False,
    ):
        self._pool = pool

        self._size_arr = np.zeros(1, dtype=np.int64)
        self._cursor_arr = np.zeros(1, dtype=np.int64)

        self._ref_count = 1
        self._lock = Lock()
        self.in_use = True

        # Initialize handles and views to None
        self._ts_h = self._off_h = self._len_h = self._buf_h = None
        self._lvl_h = self._mod_h = self._dev_h = self._seq_h = None
        self.timestamps = self.offsets = self.lengths = self.buffer = None
        self.levels = self.modules = self.devices = self.sequences = None

        self.has_levels = has_levels
        self.has_modules = has_modules
        self.has_devices = has_devices
        self.has_sequences = has_sequences

        self._allocate(req_capacity, req_buffer_kb)

    def _allocate(self, req_capacity, req_buffer_kb):
        """Acquires memory slabs and automatically infers true capacity."""

        acquire = self._pool.acquire

        # Mandatory Columns
        self._ts_h = acquire(req_capacity, dtype=dtypes.TS_TYPE)
        self.timestamps = self._ts_h.array
        self.capacity = len(self.timestamps)  # Use true power-of-two capacity

        self._off_h = acquire(self.capacity, dtype=dtypes.OFFSET_TYPE)
        self.offsets = self._off_h.array

        self._len_h = acquire(self.capacity, dtype=dtypes.LEN_TYPE)
        self.lengths = self._len_h.array

        self._buf_h = acquire(req_buffer_kb * 1024, dtype=dtypes.BYTE)
        self.buffer = self._buf_h.array

        # Optional Columns using centralized dtypes
        if self.has_levels:
            self._lvl_h = acquire(self.capacity, dtype=dtypes.LEVEL_TYPE)
            self.levels = self._lvl_h.array

        if self.has_modules:
            self._mod_h = acquire(self.capacity, dtype=dtypes.ID_TYPE)
            self.modules = self._mod_h.array

        if self.has_devices:
            self._dev_h = acquire(self.capacity, dtype=dtypes.ID_TYPE)
            self.devices = self._dev_h.array

        if self.has_sequences:
            self._seq_h = acquire(self.capacity, dtype=dtypes.SEQ_TYPE)
            self.sequences = self._seq_h.array

    @property
    def size(self):
        return self._size_arr[0]

    @size.setter
    def size(self, value):
        self._size_arr[0] = value

    @property
    def msg_cursor(self):
        return self._cursor_arr[0]

    @msg_cursor.setter
    def msg_cursor(self, value):
        self._cursor_arr[0] = value

    def bundle(self):
        return LogBundle(
            timestamps=self.timestamps,
            offsets=self.offsets,
            lengths=self.lengths,
            buffer=self.buffer,
            levels=self.levels if self.has_levels else self.EMPTY_U8,
            modules=self.modules if self.has_modules else self.EMPTY_U16,
            devices=self.devices if self.has_devices else self.EMPTY_U16,
            sequences=self.sequences if self.has_sequences else self.EMPTY_U64,
            size=self._size_arr,
            msg_cursor=self._cursor_arr,
            has_levels=self.has_levels,
            has_modules=self.has_modules,
            has_devices=self.has_devices,
            has_sequences=self.has_sequences,
        )

    def clear(self):
        """O(1) reset."""
        self._size_arr[0] = 0
        self._cursor_arr[0] = 0

    def insert(
        self, ts_ns: int, msg_bytes: bytes = b"", level: int = 0, module: int = 0, device: int = 0, seq: int = 0
    ) -> bool:
        """
        Starts a new log entry. If msg_bytes is provided, it acts as the initial chunk.
        """
        if self.size >= self.capacity:
            return False

        msg_len = len(msg_bytes)
        if self.msg_cursor + msg_len > len(self.buffer):
            return False

        idx = self.size

        # Mandatory columns
        self.timestamps[idx] = ts_ns
        self.offsets[idx] = self.msg_cursor
        self.lengths[idx] = msg_len

        # Optional columns
        if self.has_levels:
            self.levels[idx] = level
        if self.has_modules:
            self.modules[idx] = module
        if self.has_devices:
            self.devices[idx] = device
        if self.has_sequences:
            self.sequences[idx] = seq

        # Fast contiguous buffer write (if message provided)
        if msg_len > 0:
            self.buffer[self.msg_cursor : self.msg_cursor + msg_len] = np.frombuffer(msg_bytes, dtype=dtypes.BYTE)
            self.msg_cursor += msg_len

        self.size += 1
        return True

    def append(self, msg_bytes: bytes) -> bool:
        """
        Continues the message data for the MOST RECENTLY inserted entry.
        """
        if self.size == 0:
            # Cannot append to a record that hasn't been started with 'insert'
            return False

        msg_len = len(msg_bytes)
        if self.msg_cursor + msg_len > len(self.buffer):
            return False

        # Target the last entry
        idx = self.size - 1

        # Write to buffer
        self.buffer[self.msg_cursor : self.msg_cursor + msg_len] = np.frombuffer(msg_bytes, dtype=dtypes.BYTE)

        # Update the length for the current record and move cursor
        self.lengths[idx] += msg_len
        self.msg_cursor += msg_len
        return True

    def retain(self):
        with self._lock:
            if self._ref_count <= 0:
                raise RuntimeError("Cannot retain a released batch.")
            self._ref_count += 1
        return self

    def release(self):
        with self._lock:
            if self._ref_count <= 0:
                return

            self._ref_count -= 1
            if self._ref_count > 0:
                return

            self.clear()
            self.in_use = False

            # Return heavy arrays to NumpyArrayPool
            if self._ts_h:
                self._ts_h.release()
                self._ts_h = None
            if self._off_h:
                self._off_h.release()
                self._off_h = None
            if self._len_h:
                self._len_h.release()
                self._len_h = None
            if self._buf_h:
                self._buf_h.release()
                self._buf_h = None

            if self._lvl_h:
                self._lvl_h.release()
                self._lvl_h = None
            if self._mod_h:
                self._mod_h.release()
                self._mod_h = None
            if self._dev_h:
                self._dev_h.release()
                self._dev_h = None
            if self._seq_h:
                self._seq_h.release()
                self._seq_h = None

            # Nullify views to prevent use-after-free
            self.timestamps = self.offsets = self.lengths = self.buffer = None
            self.levels = self.modules = self.devices = self.sequences = None

    def __len__(self):
        return self.size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def buffer_len(self):
        with self._lock:
            return len(self.buffer) if self.buffer is not None else 0

    def __repr__(self):
        with self._lock:
            buffer_len = len(self.buffer) if self.buffer is not None else 0
            return f"PooledLogBatch(id={id(self):x} size={self.size} capacity={self.capacity}, buffer_used={self.msg_cursor}/{buffer_len}, ref={self._ref_count})"

    __str__ = __repr__

    def __iter__(self):
        timestamps, offsets, lens, buffer = self.timestamps, self.offsets, self.lengths, self.buffer
        levels, modules, devices, sequences = self.levels, self.modules, self.devices, self.sequences

        for i in range(self.size):
            offset = offsets[i]
            msg_bytes = buffer[offset : offset + lens[i]].tobytes()

            yield (
                timestamps[i],
                msg_bytes,
                levels[i] if levels is not None else None,
                modules[i] if modules is not None else None,
                devices[i] if devices is not None else None,
                sequences[i] if sequences is not None else None,
            )

    def iter_time_messages(self):
        timestamps, offsets, lens, buffer = self.timestamps, self.offsets, self.lengths, self.buffer
        for i in range(self.size):
            offset = offsets[i]
            msg_bytes = buffer[offset : offset + lens[i]].tobytes()
            # msg_view = memoryview(buffer[offset : offset + lens[i]])
            yield timestamps[i], msg_bytes

    @property
    def start_ts(self) -> int:
        """
        Returns the timestamp of the first message.
        If empty, returns max int64 so time-delta checks safely fail.
        """
        # 9223372036854775807 is (2**63 - 1), the max for int64
        return self.timestamps[0] if self.size > 0 else 9223372036854775807

    def __getitem__(self, index):
        """
        Allows indexed access to log rows.
        Returns the same tuple format as __iter__.
        """
        # 1. Handle Slicing (e.g., batch[1:5])
        if isinstance(index, slice):
            indices = range(*index.indices(self.size))
            return [self[i] for i in indices]

        # 2. Handle Integer Indexing
        if not isinstance(index, int):
            raise TypeError(f"Index must be an integer or slice, not {type(index).__name__}")

        # Support negative indexing (e.g., -1 for the last row)
        if index < 0:
            index += self.size

        if index < 0 or index >= self.size:
            raise IndexError("PooledLogBatch index out of range")

        # 3. Extract Row Data (SoA to AoS conversion)
        offset = self.offsets[index]
        length = self.lengths[index]
        msg_bytes = self.buffer[offset : offset + length].tobytes()

        return (
            self.timestamps[index],
            msg_bytes,
            self.levels[index] if self.has_levels else None,
            self.modules[index] if self.has_modules else None,
            self.devices[index] if self.has_devices else None,
            self.sequences[index] if self.has_sequences else None,
        )
