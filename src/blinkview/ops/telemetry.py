# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.segments import LogSegmentParams


@app_njit(inline="always")
def extract_floats_from_bytes(buffer, offset, length, out_array):
    """
    Scans a uint8 buffer directly for floats.
    Returns the number of floats successfully extracted.
    """
    count = 0
    max_floats = len(out_array)

    in_number = False
    is_negative = False
    val = 0.0
    fraction_div = 1.0
    has_decimal = False
    has_digit = False

    for i in range(offset, offset + length):
        c = buffer[i]

        is_digit = 48 <= c <= 57
        is_dot = c == 46
        is_minus = c == 45
        is_plus = c == 43

        if is_digit:
            if not in_number:
                in_number = True
                is_negative = False
                val = 0.0
                fraction_div = 1.0
                has_decimal = False
                has_digit = False

            has_digit = True
            if has_decimal:
                fraction_div *= 10.0
                val = val + (c - 48) / fraction_div
            else:
                val = val * 10.0 + (c - 48)

        elif is_dot:
            if not in_number:
                in_number = True
                is_negative = False
                val = 0.0
                fraction_div = 1.0
                has_decimal = True
                has_digit = False
            elif not has_decimal:
                has_decimal = True
            else:
                # Two dots? Terminate current number
                if has_digit:
                    out_array[count] = -val if is_negative else val
                    count += 1
                    if count >= max_floats:
                        return count
                # Reset for next potential number
                in_number = True
                is_negative = False
                val = 0.0
                fraction_div = 1.0
                has_decimal = True
                has_digit = False

        elif is_minus or is_plus:
            if in_number and has_digit:
                out_array[count] = -val if is_negative else val
                count += 1
                if count >= max_floats:
                    return count

            in_number = True
            is_negative = is_minus
            val = 0.0
            fraction_div = 1.0
            has_decimal = False
            has_digit = False

        else:
            # Any other character (space, letter, etc.) terminates the number
            if in_number:
                if has_digit:
                    out_array[count] = -val if is_negative else val
                    count += 1
                    if count >= max_floats:
                        return count
                in_number = False

    # Handle a number terminating at the exact end of the string
    if in_number and has_digit and count < max_floats:
        out_array[count] = -val if is_negative else val
        count += 1

    return count


@app_njit()
def extract_telemetry_segment(segment, target_module, start_seq, num_channels):
    """
    Finds logs, parses bytes to floats, and builds the return arrays in one pass.
    """

    count = segment.count
    timestamps = segment.timestamps
    modules = segment.modules
    seqs = segment.sequence_ids
    msg_offsets = segment.offsets
    msg_lens = segment.lengths
    msg_buffer = segment.buffer
    # Pre-allocate worst-case scenario. Numba does this very quickly.
    times = np.empty(count, dtype=np.float64)
    values = np.empty((count, num_channels), dtype=np.float64)

    valid_count = 0
    latest_seq = start_seq

    # A reusable buffer for our inline byte parser
    temp_floats = np.empty(num_channels, dtype=np.float64)

    for i in range(count):
        if modules[i] != target_module:
            continue

        seq = seqs[i]
        if seq <= start_seq:
            continue

        offset = msg_offsets[i]
        length = msg_lens[i]

        # Call the inline byte parser directly on the SoA buffer
        extracted_count = extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

        # Only keep rows that had all the required channels
        if extracted_count >= num_channels:
            times[valid_count] = timestamps[i] / 1_000_000_000.0
            for c in range(num_channels):
                values[valid_count, c] = temp_floats[c]

            valid_count += 1
            if seq > latest_seq:
                latest_seq = seq

    # Return perfectly sized slices
    return times[:valid_count], values[:valid_count], latest_seq


@app_njit()
def peek_segment_channels(seg: LogSegmentParams, target_module: int, start_seq: int):
    """
    Scans forward to find the first log from target_module that contains floats.
    """
    # Pre-allocate a generous temp buffer
    temp_floats = np.empty(256, dtype=np.float64)

    for i in range(seg.count):
        if seg.modules[i] == target_module and seg.sequence_ids[i] > start_seq:
            offset = seg.offsets[i]
            length = seg.lengths[i]

            # Use the inline extractor we moved here earlier
            extracted_count = extract_floats_from_bytes(seg.buffer, offset, length, temp_floats)

            if extracted_count > 0:
                return extracted_count

    return 0
