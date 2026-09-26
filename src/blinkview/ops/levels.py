# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import UnifiedParserConfig
from blinkview.ops.stage_loop import FS_OK, FS_STEP_FAILED
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.views import nb_log_bundle_views, nb_string_table_views, nb_view


@app_njit(inline="always")
def nb_parse_log_level(buffer, start_cursor, end_cursor, out_b, out_idx, state, unified_config: UnifiedParserConfig):
    """
    Optimized parser leveraging null-terminated reference buffer and first-byte short-circuiting.
    """
    config = unified_config.string_table
    count = config.count
    lens = config.lens
    offsets = config.offsets
    values = config.values
    ref_buffer = config.buffer

    # 1. Capture the first byte of the potential token in the stream
    if start_cursor >= end_cursor:
        return -1

    first_byte = buffer[start_cursor]

    for i in range(count):
        length = lens[i]
        offset = offsets[i]

        # 2. QUICK REJECTION
        # - Check first byte (removes ~80% of iterations for standard levels)
        # - Check bounds
        if ref_buffer[offset] != first_byte or (start_cursor + length > end_cursor):
            continue

        # 3. BYTE-BY-BYTE COMPARISON
        # We start from index 1 because we already verified index 0
        match = True
        for j in range(1, length):
            if buffer[start_cursor + j] != ref_buffer[offset + j]:
                match = False
                break

        if match:
            # 4. PREFIX PROTECTION (Using the Null Terminator Layout)
            # Since ref_buffer[offset + length] is now guaranteed to be 0,
            # we check if the input buffer is ALSO at a boundary (space, colon, or end).
            # This prevents "INFO" from matching "INFOMAN".
            next_idx = start_cursor + length
            if next_idx < end_cursor:
                trailing_char = buffer[next_idx]
                # If the next char is alphanumeric, it's a prefix match, not a full match
                # (Adjust characters 48-57, 65-90, 97-122 as needed for your format)
                if (48 <= trailing_char <= 57) or (65 <= trailing_char <= 90) or (97 <= trailing_char <= 122):
                    continue

            # 5. SUCCESS
            out_b.levels[out_idx] = values[i]

            return nb_skip_whitespace(buffer, next_idx, end_cursor)

    return -1


@app_njit(inline="always")
def nb_parse_log_level_optimized(buffer, start_cursor, end_cursor, out_b, out_idx, config):
    """
    Optimized parser leveraging null-terminated reference buffer and first-byte short-circuiting.
    """
    count = config.count
    lens = config.lens
    offsets = config.offsets
    values = config.values
    ref_buffer = config.buffer

    # 1. Capture the first byte of the potential token in the stream
    if start_cursor >= end_cursor:
        return -1

    first_byte = buffer[start_cursor]

    for i in range(count):
        length = lens[i]
        offset = offsets[i]

        # 2. QUICK REJECTION
        # - Check first byte (removes ~80% of iterations for standard levels)
        # - Check bounds
        if ref_buffer[offset] != first_byte or (start_cursor + length > end_cursor):
            continue

        # 3. BYTE-BY-BYTE COMPARISON
        # We start from index 1 because we already verified index 0
        match = True
        for j in range(1, length):
            if buffer[start_cursor + j] != ref_buffer[offset + j]:
                match = False
                break

        if match:
            # 4. PREFIX PROTECTION (Using the Null Terminator Layout)
            # Since ref_buffer[offset + length] is now guaranteed to be 0,
            # we check if the input buffer is ALSO at a boundary (space, colon, or end).
            # This prevents "INFO" from matching "INFOMAN".
            next_idx = start_cursor + length
            if next_idx < end_cursor:
                trailing_char = buffer[next_idx]
                # If the next char is alphanumeric, it's a prefix match, not a full match
                # (Adjust characters 48-57, 65-90, 97-122 as needed for your format)
                if (48 <= trailing_char <= 57) or (65 <= trailing_char <= 90) or (97 <= trailing_char <= 122):
                    continue

            # 5. SUCCESS
            out_b.levels[out_idx] = values[i]

            return nb_skip_whitespace(buffer, next_idx, end_cursor)

    return -1


@app_njit()
def nb_parse_log_level_stage(out_b0, f_state, n, table_bundle):
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    table = nb_string_table_views(table_bundle)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = nb_parse_log_level_optimized(buffer, cur[k], end[k], out_b, first + k, table)
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r
