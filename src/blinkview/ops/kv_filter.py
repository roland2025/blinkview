# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core.dtypes import BYTE
from blinkview.core.numba_config import app_njit

# Conditions are tracked with a bitmask (one bit per condition), so this is a hard cap on how
# many "key=value" terms a single logfmt filter query can carry.
MAX_KV_CONDITIONS = 8

_OFFSET_TYPE = np.uint32
_LEN_TYPE = np.uint32

EMPTY_KV_BYTES = np.frombuffer(b"", dtype=BYTE)  # read-only, matching build_kv_condition_arrays' real cond_*_buf
EMPTY_KV_OFFSETS = np.empty(0, dtype=_OFFSET_TYPE)
EMPTY_KV_LENGTHS = np.empty(0, dtype=_LEN_TYPE)


class KvConditionArrays(NamedTuple):
    """Flattened logfmt "key=value" conditions, ready to pass into the Numba filter kernels."""

    cond_keys_buf: np.ndarray
    cond_keys_off: np.ndarray
    cond_keys_len: np.ndarray
    cond_vals_buf: np.ndarray
    cond_vals_off: np.ndarray
    cond_vals_len: np.ndarray
    num_conditions: int


EMPTY_KV_CONDITIONS = KvConditionArrays(
    EMPTY_KV_BYTES, EMPTY_KV_OFFSETS, EMPTY_KV_LENGTHS, EMPTY_KV_BYTES, EMPTY_KV_OFFSETS, EMPTY_KV_LENGTHS, 0
)


def build_kv_condition_arrays(conditions: list[tuple[bytes, bytes]]) -> KvConditionArrays:
    """Flattens a list of (key_bytes, value_bytes) pairs into the flat buffer/offset/length
    arrays nb_row_matches_kv_conditions (and the filter kernels that call it) expect. Pure
    Python - this runs once per filter-text change, not per row."""
    if not conditions:
        return EMPTY_KV_CONDITIONS

    conditions = conditions[:MAX_KV_CONDITIONS]

    keys_buf = bytearray()
    keys_off = []
    keys_len = []
    vals_buf = bytearray()
    vals_off = []
    vals_len = []

    for key, value in conditions:
        keys_off.append(len(keys_buf))
        keys_len.append(len(key))
        keys_buf.extend(key)

        vals_off.append(len(vals_buf))
        vals_len.append(len(value))
        vals_buf.extend(value)

    return KvConditionArrays(
        cond_keys_buf=np.frombuffer(bytes(keys_buf), dtype=BYTE) if keys_buf else EMPTY_KV_BYTES,
        cond_keys_off=np.array(keys_off, dtype=_OFFSET_TYPE),
        cond_keys_len=np.array(keys_len, dtype=_LEN_TYPE),
        cond_vals_buf=np.frombuffer(bytes(vals_buf), dtype=BYTE) if vals_buf else EMPTY_KV_BYTES,
        cond_vals_off=np.array(vals_off, dtype=_OFFSET_TYPE),
        cond_vals_len=np.array(vals_len, dtype=_LEN_TYPE),
        num_conditions=len(conditions),
    )


@app_njit(inline="always")
def nb_is_ws(c) -> bool:
    return c == 32 or c == 9 or c == 10 or c == 13


@app_njit()
def nb_row_matches_kv_conditions(
    buffer,
    offset,
    length,
    cond_keys_buf,
    cond_keys_off,
    cond_keys_len,
    cond_vals_buf,
    cond_vals_off,
    cond_vals_len,
    num_conditions,
    field_delim_int,
    kv_delim_int,
) -> bool:
    """Tokenizes buffer[offset:offset+length] into logfmt "key=value" pairs (same delimiter
    scan / first-'='-split / whitespace-trim / matching-quote-strip algorithm as
    KeyValueExtractionRule.process, reimplemented as typed Numba loops with no Python
    objects/allocation) and checks whether every one of the (up to MAX_KV_CONDITIONS) supplied
    conditions is satisfied by some pair in the row. Conditions are ANDed; satisfaction is
    tracked with a bitmask so the scan can early-exit the instant every condition is met."""
    if num_conditions == 0:
        return True

    start = offset
    end = offset + length

    full_mask = (1 << num_conditions) - 1
    satisfied_mask = 0

    chunk_start = start
    kv_pos = -1
    in_quote = 0  # 0 = none, else the quote byte (34 = ", 39 = ')

    for j in range(start, end + 1):
        c = buffer[j] if j < end else field_delim_int

        if (c == 34 or c == 39) and kv_pos != -1 and j < end:
            if in_quote == 0:
                in_quote = c
            elif in_quote == c:
                in_quote = 0

        if c == kv_delim_int and kv_pos == -1 and j < end:
            kv_pos = j
        elif (c == field_delim_int and in_quote == 0) or j == end:
            if kv_pos != -1:
                k_start = chunk_start
                k_end = kv_pos
                while k_start < k_end and nb_is_ws(buffer[k_start]):
                    k_start += 1
                while k_end > k_start and nb_is_ws(buffer[k_end - 1]):
                    k_end -= 1

                v_start = kv_pos + 1
                v_end = j
                while v_start < v_end and nb_is_ws(buffer[v_start]):
                    v_start += 1
                while v_end > v_start and nb_is_ws(buffer[v_end - 1]):
                    v_end -= 1

                if v_start < v_end and (buffer[v_start] == 34 or buffer[v_start] == 39):
                    quote_char = buffer[v_start]
                    if buffer[v_end - 1] == quote_char:
                        v_start += 1
                        v_end -= 1

                if k_start < k_end and v_start < v_end:
                    k_len = k_end - k_start
                    v_len = v_end - v_start

                    for ci in range(num_conditions):
                        bit = 1 << ci
                        if satisfied_mask & bit:
                            continue

                        if cond_keys_len[ci] != k_len:
                            continue
                        key_eq = True
                        for bidx in range(k_len):
                            if buffer[k_start + bidx] != cond_keys_buf[cond_keys_off[ci] + bidx]:
                                key_eq = False
                                break
                        if not key_eq:
                            continue

                        if cond_vals_len[ci] != v_len:
                            continue
                        val_eq = True
                        for bidx in range(v_len):
                            if buffer[v_start + bidx] != cond_vals_buf[cond_vals_off[ci] + bidx]:
                                val_eq = False
                                break
                        if val_eq:
                            satisfied_mask |= bit

                    if satisfied_mask == full_mask:
                        return True

            chunk_start = j + 1
            kv_pos = -1
            in_quote = 0

    return satisfied_mask == full_mask
