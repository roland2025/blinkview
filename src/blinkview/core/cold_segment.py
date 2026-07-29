# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""On-disk format for archived (cold) CircularLogPool segments - see plans/mmap-coldstore.md.

A frozen segment (one that has stopped being CircularLogPool.active_segment - see
core/numpy_log.py) never changes again, so it's written at its *actual* used size (bundle.size[0]
rows, bundle.msg_cursor[0] buffer bytes) rather than its padded pool-slab capacity.

Layout (see write_cold_segment_file):
    [fixed header: magic, version, row_count, buffer_len, first_seq, last_seq, earliest_ts,
     latest_ts]
    [column table: one (offset: uint64, length_bytes: uint64) entry per COLUMN_SPECS entry]
    [pad to HEADER_SIZE]
    [column data blocks, each at its table-recorded offset/length]

The table (not a hardcoded write order) is what a reader uses to find each column - see
open_cold_segment_arrays / _MmapFileRef.view. Schema (which columns exist, their dtypes) is
hardcoded to CircularLogPool's known segment shape (has_levels/modules/devices/sequences/pids/
tids=True, no ext_u32/u64 - _rotate_segment() never sets those) rather than a generic serializer:
there is exactly one producer of these files.
"""

import mmap
import struct
import threading
from pathlib import Path
from typing import Dict, NamedTuple, Tuple, Union

import numpy as np

from blinkview.core import dtypes

MAGIC = b"BVCOLD01"
VERSION = 1
HEADER_SIZE = 4096

# magic(8s), version(I), row_count(q), buffer_len(q), first_seq(Q), last_seq(Q), earliest_ts(q),
# latest_ts(q)
_FIXED_HEADER_FMT = "<8sIqqQQqq"
_FIXED_HEADER_SIZE = struct.calcsize(_FIXED_HEADER_FMT)

_TABLE_ENTRY_FMT = "<QQ"  # (offset, length_bytes)
_TABLE_ENTRY_SIZE = struct.calcsize(_TABLE_ENTRY_FMT)

# Column order persisted to disk. "buffer" is sized in bytes directly (buffer_len); every other
# column is sized by row_count.
COLUMN_SPECS: Tuple[Tuple[str, type], ...] = (
    ("timestamps", dtypes.TS_TYPE),
    ("rx_timestamps", dtypes.TS_TYPE),
    ("offsets", dtypes.OFFSET_TYPE),
    ("lengths", dtypes.LEN_TYPE),
    ("levels", dtypes.LEVEL_TYPE),
    ("modules", dtypes.ID_TYPE),
    ("devices", dtypes.ID_TYPE),
    ("sequences", dtypes.SEQ_TYPE),
    ("pids", dtypes.ID_TYPE),
    ("tids", dtypes.ID_TYPE),
    ("buffer", dtypes.BYTE),
)

_TABLE_SIZE = _TABLE_ENTRY_SIZE * len(COLUMN_SPECS)

if _FIXED_HEADER_SIZE + _TABLE_SIZE > HEADER_SIZE:
    raise RuntimeError("cold segment header layout no longer fits HEADER_SIZE")


class ColdSegmentMeta(NamedTuple):
    """Set as PooledLogBatch.metadata for a cold (memmap-backed) segment - see
    PooledLogBatch.from_memmap and ColdStorageArchiver. Carries just enough of the header to
    answer "is this segment in range" (CircularLogPool.get_time_bounds, and the fetch-scaling
    fixes in plans/fetch-telemetry-window-cold-segment-perf.md), "what are its seq bounds"
    (PooledLogBatch.first_sequence_id/last_sequence_id), and "which file do I delete on eviction"
    - all without touching (page-faulting in) any of the mmap'd column data."""

    path: str
    earliest_ts: int
    latest_ts: int
    first_seq: int
    last_seq: int


class ColdSegmentHeader:
    __slots__ = ("row_count", "buffer_len", "first_seq", "last_seq", "earliest_ts", "latest_ts", "table")

    def __init__(self, row_count, buffer_len, first_seq, last_seq, earliest_ts, latest_ts, table):
        self.row_count = row_count
        self.buffer_len = buffer_len
        self.first_seq = first_seq
        self.last_seq = last_seq
        self.earliest_ts = earliest_ts
        self.latest_ts = latest_ts
        self.table = table  # tuple of (offset, length_bytes), aligned with COLUMN_SPECS


def write_cold_segment_file(path: Union[str, Path], bundle) -> ColdSegmentHeader:
    """Serializes a frozen LogBundle to `path`. `bundle` must not be mutated concurrently -
    callers archive only already-rotated-out segments (see CircularLogPool._rotate_segment),
    which by construction never receive more writes. Writes to a `.tmp` sibling and renames into
    place so a reader can never observe a partially-written file at `path`."""
    row_count = int(bundle.size[0])
    buffer_len = int(bundle.msg_cursor[0])

    if row_count > 0 and bundle.has_sequences:
        first_seq = int(bundle.sequences[0])
        last_seq = int(bundle.sequences[row_count - 1])
    else:
        first_seq = 0
        last_seq = 0

    if row_count > 0:
        earliest_ts = int(bundle.timestamps[0])
        latest_ts = int(bundle.timestamps[row_count - 1])
    else:
        earliest_ts = 0
        latest_ts = 0

    arrays = []
    for name, _dt in COLUMN_SPECS[:-1]:
        arrays.append(getattr(bundle, name)[:row_count])
    arrays.append(bundle.buffer[:buffer_len])

    table = []
    cursor = HEADER_SIZE
    for arr in arrays:
        length_bytes = int(arr.nbytes)
        table.append((cursor, length_bytes))
        cursor += length_bytes

    header_bytes = struct.pack(
        _FIXED_HEADER_FMT, MAGIC, VERSION, row_count, buffer_len, first_seq, last_seq, earliest_ts, latest_ts
    )
    table_bytes = b"".join(struct.pack(_TABLE_ENTRY_FMT, off, length) for off, length in table)
    padding = b"\x00" * (HEADER_SIZE - len(header_bytes) - len(table_bytes))

    path = Path(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "wb") as f:
        f.write(header_bytes)
        f.write(table_bytes)
        f.write(padding)
        for arr in arrays:
            f.write(np.ascontiguousarray(arr).tobytes())

    tmp_path.replace(path)

    return ColdSegmentHeader(row_count, buffer_len, first_seq, last_seq, earliest_ts, latest_ts, tuple(table))


def read_cold_segment_header(path: Union[str, Path]) -> ColdSegmentHeader:
    with open(path, "rb") as f:
        header_bytes = f.read(_FIXED_HEADER_SIZE)
        magic, version, row_count, buffer_len, first_seq, last_seq, earliest_ts, latest_ts = struct.unpack(
            _FIXED_HEADER_FMT, header_bytes
        )
        if magic != MAGIC:
            raise ValueError(f"Not a cold segment file (bad magic): {path}")
        if version != VERSION:
            raise ValueError(f"Unsupported cold segment version {version} in {path}")

        table_bytes = f.read(_TABLE_SIZE)
        table = tuple(
            struct.unpack(_TABLE_ENTRY_FMT, table_bytes[i * _TABLE_ENTRY_SIZE : (i + 1) * _TABLE_ENTRY_SIZE])
            for i in range(len(COLUMN_SPECS))
        )

    return ColdSegmentHeader(row_count, buffer_len, first_seq, last_seq, earliest_ts, latest_ts, table)


class _MmapFileRef:
    """Refcounted holder of one shared copy-on-write mmap over a cold segment file. Every column
    is a np.frombuffer view into this single mapping (not a separate np.memmap per column), so
    releasing all of a segment's array handles closes exactly one mmap/file object.

    Uses ACCESS_COPY rather than ACCESS_READ: np.frombuffer's writeable flag mirrors the
    underlying buffer's, so an ACCESS_READ mapping produces a *read-only* array - a different
    Numba array type (`readonly array(...)`) than the writable pool-backed arrays every segment
    kernel is normally called with (see .claude/skills/numba-njit/SKILL.md #15). That silently
    forces a second compiled specialization of every kernel touching cold-segment data (visible as
    `[cache] ... saved` lines for kernels that were already warmed) the first time playback scrubs
    into cold storage. ACCESS_COPY keeps the mapping's pages copy-on-write - the array comes back
    writable (matching the live-pool type) but no code here ever writes through it, and nothing
    is ever flushed back to the file."""

    def __init__(self, path: Union[str, Path]):
        self._file = open(path, "rb")
        try:
            self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_COPY)
        except Exception:
            self._file.close()
            raise
        self._refcount = 0
        self._lock = threading.Lock()

    def retain(self) -> "_MmapFileRef":
        with self._lock:
            self._refcount += 1
        return self

    def release(self) -> None:
        with self._lock:
            self._refcount -= 1
            if self._refcount > 0:
                return
        self._mmap.close()
        self._file.close()

    def view(self, dtype, offset: int, length_bytes: int) -> np.ndarray:
        if length_bytes == 0:
            return np.empty(0, dtype=dtype)
        count = length_bytes // np.dtype(dtype).itemsize
        return np.frombuffer(self._mmap, dtype=dtype, count=count, offset=offset)


class MmapArrayHandle:
    """Drop-in stand-in for core.array_pool.PooledArrayHandle (same `.array`/`.release()`
    surface) so PooledLogBatch.release()'s existing per-handle teardown loop works unmodified
    against a memmap-backed segment - see PooledLogBatch.from_memmap."""

    __slots__ = ("array", "_file_ref")

    def __init__(self, array: np.ndarray, file_ref: _MmapFileRef):
        self.array = array
        self._file_ref = file_ref
        file_ref.retain()

    def release(self) -> None:
        self.array = None
        if self._file_ref is not None:
            self._file_ref.release()
            self._file_ref = None


def open_cold_segment_arrays(path: Union[str, Path]) -> Tuple[ColdSegmentHeader, Dict[str, MmapArrayHandle]]:
    """Opens a cold segment file, returning its header and one MmapArrayHandle per COLUMN_SPECS
    column, all sharing a single retained _MmapFileRef."""
    header = read_cold_segment_header(path)
    file_ref = _MmapFileRef(path)

    handles: Dict[str, MmapArrayHandle] = {}
    for (name, dt), (offset, length_bytes) in zip(COLUMN_SPECS, header.table):
        arr = file_ref.view(dt, offset, length_bytes)
        handles[name] = MmapArrayHandle(arr, file_ref)

    return header, handles
