# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.types import RegistryParams, StringTableParams
from blinkview.core.numba_config import app_njit
from blinkview.core.types.formatting import FormattingConfig
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.segments import LogSegmentParams
from blinkview.ops.constants import (
    CHAR_COLON,
    CHAR_DASH,
    CHAR_DOT,
    CHAR_LF,
    CHAR_NULL,
    CHAR_QUESTION,
    CHAR_SPACE,
    CHAR_T,
    CHAR_Z,
    CHAR_ZERO,
)


@app_njit()
def update_iso8601_timestamp_cache(total_sec: int, ts_cache: np.ndarray):
    """
    Computes the ISO8601 date and time (YYYY-MM-DDTHH:MM:SS) from Unix epoch seconds
    and updates the provided 19-byte numpy array in-place.
    """

    days = total_sec // 86400
    sec_of_day = total_sec % 86400

    hr = sec_of_day // 3600
    mn = (sec_of_day % 3600) // 60
    sec = sec_of_day % 60

    z = days + 719468
    era = (z if z >= 0 else z - 146096) // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + (3 if mp < 10 else -9)
    y += 1 if m <= 2 else 0

    # Update the 19-byte date/time cache array
    ts_cache[0] = CHAR_ZERO + (y // 1000)
    ts_cache[1] = CHAR_ZERO + ((y // 100) % 10)
    ts_cache[2] = CHAR_ZERO + ((y // 10) % 10)
    ts_cache[3] = CHAR_ZERO + (y % 10)
    ts_cache[4] = CHAR_DASH
    ts_cache[5] = CHAR_ZERO + (m // 10)
    ts_cache[6] = CHAR_ZERO + (m % 10)
    ts_cache[7] = CHAR_DASH
    ts_cache[8] = CHAR_ZERO + (d // 10)
    ts_cache[9] = CHAR_ZERO + (d % 10)
    ts_cache[10] = CHAR_T
    ts_cache[11] = CHAR_ZERO + (hr // 10)
    ts_cache[12] = CHAR_ZERO + (hr % 10)
    ts_cache[13] = CHAR_COLON
    ts_cache[14] = CHAR_ZERO + (mn // 10)
    ts_cache[15] = CHAR_ZERO + (mn % 10)
    ts_cache[16] = CHAR_COLON
    ts_cache[17] = CHAR_ZERO + (sec // 10)
    ts_cache[18] = CHAR_ZERO + (sec % 10)


@app_njit()
def estimate_log_batch_size(
    indices: np.ndarray,
    segment: LogSegmentParams,
    tables: RegistryParams,
    cfg: FormattingConfig,
) -> int:
    # Unpack registry lengths
    _, _, l_len, _, _, l_count = tables.levels
    _, _, m_len, _, _, m_count = tables.modules
    _, _, d_len, _, _, d_count = tables.devices

    # Unpack segment metadata
    s_lens = segment.lengths
    s_devs = segment.devices
    s_lvls = segment.levels
    s_mods = segment.modules

    show_ts, show_dev = cfg.show_ts, cfg.show_dev
    show_lvl, show_mod = cfg.show_lvl, cfg.show_mod

    total_size = 0
    for idx in indices:
        row_size = 0
        is_first = True

        if show_ts:
            row_size += 12  # HH:MM:SS.mmm
            is_first = False

        if show_dev:
            if not is_first:
                row_size += 1
            d_id = s_devs[idx]
            row_size += d_len[d_id] if d_id < d_count else 3
            is_first = False

        if show_lvl:
            if not is_first:
                row_size += 1
            l_id = s_lvls[idx]
            row_size += l_len[l_id] if l_id < l_count else 3
            is_first = False

        if show_mod:
            if not is_first:
                row_size += 1
            m_id = s_mods[idx]
            row_size += (m_len[m_id] if m_id < m_count else 7) + 1  # Name + ":"
            is_first = False

        # Message: space (if needed) + content + newline
        if not is_first:
            row_size += 1
        row_size += s_lens[idx] + 1

        total_size += row_size

    return total_size


@app_njit(inline="always")
def copy_bytes(out: np.ndarray, curr: int, src: np.ndarray, off: int, ln: int) -> int:
    """Explicit loop copy for short strings."""
    for i in range(ln):
        out[curr + i] = src[off + i]
    return curr + ln


@app_njit(inline="always")
def write_bytes_direct(out: np.ndarray, curr: int, data: tuple) -> int:
    """Writes a fixed tuple of bytes (like fallbacks)."""
    for i in range(len(data)):
        out[curr + i] = data[i]
    return curr + len(data)


@app_njit(inline="always")
def write_table_entry(out: np.ndarray, curr: int, table: StringTableParams, idx: int, fallback: tuple) -> int:
    """Helper for Device/Module where the ID is the index."""
    if idx < table.count:
        return copy_bytes(out, curr, table.buffer, table.offsets[idx], table.lens[idx])
    return write_bytes_direct(out, curr, fallback)


@app_njit(inline="always")
def write_table_lookup(out: np.ndarray, curr: int, table: StringTableParams, raw_id: int, fallback: tuple) -> int:
    """Helper for Levels where we must find the index first."""
    tbl_idx = find_id_index(table.values, table.count, raw_id)
    if tbl_idx != -1:
        return copy_bytes(out, curr, table.buffer, table.offsets[tbl_idx], table.lens[tbl_idx])
    return write_bytes_direct(out, curr, fallback)


@app_njit(inline="always")
def find_id_index(val_arr: np.ndarray, count: int, target_id: int) -> int:
    """Returns the internal index for a given identity ID, or -1 if not found."""
    for i in range(count):
        if val_arr[i] == target_id:
            return i
    return -1


@app_njit()
def format_log_batch(
    out: np.ndarray,
    indices: np.ndarray,
    segment: LogSegmentParams,
    tables: RegistryParams,
    cfg: FormattingConfig,
    tz_offset_sec: int,
):
    # 1. Unpack Tables
    l_buf, l_off, l_len, _, l_values, l_count = tables.levels
    m_buf, m_off, m_len, _, _, m_count = tables.modules
    d_buf, d_off, d_len, _, _, d_count = tables.devices

    # 2. Unpack Segment (Crucial for Numba stability)
    s_ts = segment.timestamps
    s_lvls = segment.levels
    s_mods = segment.modules
    s_devs = segment.devices
    s_offs = segment.offsets
    s_lens = segment.lengths
    s_buf = segment.buffer

    show_ts, show_dev = cfg.show_ts, cfg.show_dev
    show_lvl, show_mod = cfg.show_lvl, cfg.show_mod
    tz_offset_ns = tz_offset_sec * 1_000_000_000
    # Fallbacks
    UNKNOWN_LEVEL = (CHAR_QUESTION, CHAR_QUESTION, CHAR_QUESTION)
    UNKNOWN_DEV = (CHAR_QUESTION, CHAR_QUESTION, CHAR_QUESTION)
    UNKNOWN_MOD = (117, 110, 107, 110, 111, 119, 110)  # "unknown"

    curr = 0
    for idx in indices:
        first_field = True

        # 1. Timestamp
        if show_ts:
            ts_ns = s_ts[idx] + tz_offset_ns
            ms = (ts_ns // 1_000_000) % 1000
            sec = (ts_ns // 1_000_000_000) % 60
            mn = (ts_ns // 60_000_000_000) % 60
            hr = (ts_ns // 3_600_000_000_000) % 24

            out[curr + 0], out[curr + 1] = CHAR_ZERO + (hr // 10), CHAR_ZERO + (hr % 10)
            out[curr + 2] = CHAR_COLON
            out[curr + 3], out[curr + 4] = CHAR_ZERO + (mn // 10), CHAR_ZERO + (mn % 10)
            out[curr + 5] = CHAR_COLON
            out[curr + 6], out[curr + 7] = CHAR_ZERO + (sec // 10), CHAR_ZERO + (sec % 10)
            out[curr + 8] = CHAR_DOT
            out[curr + 9], out[curr + 10], out[curr + 11] = (
                CHAR_ZERO + (ms // 100),
                CHAR_ZERO + ((ms // 10) % 10),
                CHAR_ZERO + (ms % 10),
            )
            curr += 12
            first_field = False

        # --- 2. Device (Direct Index) ---
        if show_dev:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            curr = write_table_entry(out, curr, tables.devices, s_devs[idx], UNKNOWN_DEV)
            first_field = False

        # --- 3. Level (Search Lookup) ---
        if show_lvl:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            curr = write_table_lookup(out, curr, tables.levels, s_lvls[idx], UNKNOWN_LEVEL)
            first_field = False

        # --- 4. Module (Direct Index) ---
        if show_mod:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            curr = write_table_entry(out, curr, tables.modules, s_mods[idx], UNKNOWN_MOD)
            out[curr] = CHAR_COLON
            curr += 1
            first_field = False

        # --- 5. Message ---
        if not first_field:
            out[curr] = CHAR_SPACE
            curr += 1

        curr = copy_bytes(out, curr, s_buf, s_offs[idx], s_lens[idx])

        out[curr] = CHAR_LF
        curr += 1

    return curr


@app_njit()
def estimate_batch_capacity(bundle: LogBundle, overhead_per_row: int) -> int:
    """
    O(1) capacity estimation.
    Uses the pre-calculated msg_cursor (total u8 bytes) and
    batch size to determine required output buffer size.
    """
    # msg_cursor[0] == sum(lengths[:size])
    return bundle.msg_cursor[0] + (overhead_per_row * bundle.size[0])


@app_njit()
def format_log_row_batch(
    out: np.ndarray,
    bundle: LogBundle,
    tables: RegistryParams,
    sec_state: np.ndarray,
    ts_cache: np.ndarray,
) -> int:
    # 1. Localize state and unpack metadata
    size = bundle.size[0]
    if size == 0:
        return 0

    last_sec = sec_state[0]

    # Unpack Segment
    s_ts, s_lvls = bundle.timestamps, bundle.levels
    s_mods, s_devs = bundle.modules, bundle.devices
    s_offs, s_lens, s_buf = bundle.offsets, bundle.lengths, bundle.buffer

    # Constant Fallbacks
    UNKNOWN_LEVEL = (CHAR_QUESTION, CHAR_QUESTION, CHAR_QUESTION)
    UNKNOWN_DEV = (CHAR_QUESTION, CHAR_QUESTION, CHAR_QUESTION)
    UNKNOWN_MOD = (117, 110, 107, 110, 111, 119, 110)  # "unknown"

    curr = 0
    for idx in range(size):
        # --- 1. Timestamp ---
        ts_ns = s_ts[idx]
        total_sec = ts_ns // 1_000_000_000

        # Update cache using localized state
        if total_sec != last_sec:
            update_iso8601_timestamp_cache(total_sec, ts_cache)
            last_sec = total_sec

        # Copy 19 bytes: YYYY-MM-DDTHH:MM:SS
        for i in range(19):
            out[curr + i] = ts_cache[i]
        curr += 19

        # --- Microseconds (.uuuuuuZ ) ---
        us = (ts_ns // 1_000) % 1_000_000

        out[curr] = CHAR_DOT
        out[curr + 1] = CHAR_ZERO + (us // 100000)
        out[curr + 2] = CHAR_ZERO + ((us // 10000) % 10)
        out[curr + 3] = CHAR_ZERO + ((us // 1000) % 10)
        out[curr + 4] = CHAR_ZERO + ((us // 100) % 10)
        out[curr + 5] = CHAR_ZERO + ((us // 10) % 10)
        out[curr + 6] = CHAR_ZERO + (us % 10)
        out[curr + 7] = CHAR_Z
        out[curr + 8] = CHAR_SPACE
        curr += 9

        # --- 2. Level (Lookup) ---
        curr = write_table_lookup(out, curr, tables.levels, s_lvls[idx], UNKNOWN_LEVEL)
        out[curr] = CHAR_SPACE
        curr += 1

        # --- 3. Device (Direct) ---
        curr = write_table_entry(out, curr, tables.devices, s_devs[idx], UNKNOWN_DEV)
        out[curr] = CHAR_SPACE
        curr += 1

        # --- 4. Module (Direct) ---
        curr = write_table_entry(out, curr, tables.modules, s_mods[idx], UNKNOWN_MOD)
        out[curr], out[curr + 1] = CHAR_COLON, CHAR_SPACE
        curr += 2

        # --- 5. Message ---
        curr = copy_bytes(out, curr, s_buf, s_offs[idx], s_lens[idx])
        out[curr] = CHAR_LF
        curr += 1

    # 2. Persist the final state back to the array for the next batch
    sec_state[0] = last_sec
    return curr


@app_njit(inline="always")
def set_u32_le(arr: np.ndarray, offset: int, value: int):
    """Writes a 32-bit unsigned integer in Little Endian."""
    arr[offset] = value & 0xFF
    arr[offset + 1] = (value >> 8) & 0xFF
    arr[offset + 2] = (value >> 16) & 0xFF
    arr[offset + 3] = (value >> 24) & 0xFF


@app_njit(inline="always")
def set_u64_le(arr: np.ndarray, offset: int, value: int):
    """Writes a 64-bit unsigned integer in Little Endian."""
    arr[offset] = value & 0xFF
    arr[offset + 1] = (value >> 8) & 0xFF
    arr[offset + 2] = (value >> 16) & 0xFF
    arr[offset + 3] = (value >> 24) & 0xFF
    arr[offset + 4] = (value >> 32) & 0xFF
    arr[offset + 5] = (value >> 40) & 0xFF
    arr[offset + 6] = (value >> 48) & 0xFF
    arr[offset + 7] = (value >> 56) & 0xFF


# Binary Format Constants
BIN_SYNC = 0xA5
BIN_TYPE_DATA = 0x01
BIN_VERSION = 0x01
BIN_HEADER_SIZE = 16


@app_njit()
def format_binary_batch(out: np.ndarray, bundle: LogBundle) -> int:
    """
    Serializes a LogBundle of binary logs into the `out` array using helpers.
    Header: Sync(1), Type(1), Ver(1), Res(1), Len(4), TS(8) = 16 bytes.
    """
    timestamps = bundle.timestamps
    offsets = bundle.offsets
    lengths = bundle.lengths
    buffer = bundle.buffer
    size = bundle.size[0]

    curr = 0
    for i in range(size):
        ts = timestamps[i]
        length = lengths[i]
        off = offsets[i]

        # --- 1. Header (Fixed Fields) ---
        out[curr] = BIN_SYNC
        out[curr + 1] = BIN_TYPE_DATA
        out[curr + 2] = BIN_VERSION
        out[curr + 3] = CHAR_NULL  # 0x00

        # --- 2. Header (Variable Fields via Helpers) ---
        set_u32_le(out, curr + 4, length)
        set_u64_le(out, curr + 8, ts)

        curr += BIN_HEADER_SIZE

        # --- 3. Payload (Explicit Loop) ---
        for j in range(length):
            out[curr + j] = buffer[off + j]
        curr += length

    return curr
