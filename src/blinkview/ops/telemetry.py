# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import TelemetryBatch
from blinkview.core.types.segments import LogSegmentParams
from blinkview.core.types.telemetry import TelemetryBufferBundle


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
def extract_telemetry_segment_to_end(
    segment: LogSegmentParams,
    target_module: int,
    start_seq: dtypes.SEQ_TYPE,
    num_channels: int,
    out_times: np.ndarray,  # Full max_points size
    out_times_int64: np.ndarray,
    out_values: np.ndarray,  # Full max_points size
    temp_floats: np.ndarray,
    write_idx: int,  # Starting write position (moves backward)
) -> int:
    """
    Fills arrays from write_idx backwards.
    Returns the updated write_idx.
    """
    # Metadata for the segment
    count = segment.count[0]
    timestamps = segment.timestamps
    modules = segment.modules
    seqs = segment.sequence_ids
    msg_offsets = segment.offsets
    msg_lens = segment.lengths
    msg_buffer = segment.buffer

    # Scan segment newest -> oldest
    for i in range(count - 1, -1, -1):
        # Stop if we hit the start of our pre-allocated buffer
        if write_idx <= 0:
            break

        # Stop if we hit the watermark (data we already have)
        seq = seqs[i]
        if seq <= start_seq and start_seq != SEQ_NONE:
            break

        if modules[i] != target_module:
            continue

        # Extract telemetry
        offset = msg_offsets[i]
        length = msg_lens[i]
        extracted_count = extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

        if extracted_count >= num_channels:
            # Move pointer left and write
            write_idx -= 1
            ts_int = timestamps[i]
            out_times[write_idx] = ts_int / 1_000_000_000.0
            out_times_int64[write_idx] = ts_int

            # Write channels
            for c in range(num_channels):
                out_values[write_idx, c] = temp_floats[c]

    return write_idx


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


@app_njit()
def peek_segment_channels_backwards(
    seg: LogSegmentParams, target_module: int, start_seq: dtypes.SEQ_TYPE, temp_floats: np.ndarray
):
    """
    Scans BACKWARDS to find the LATEST log from target_module containing telemetry.
    Returns: (found_sequence_id, channel_count)
    """
    seg_count = seg.count[0]
    for i in range(seg_count - 1, -1, -1):
        # Early exit: if this log is already older than our filter, stop scanning
        if seg.sequence_ids[i] <= start_seq:
            break

        if seg.modules[i] == target_module:
            # Use the actual attribute names from your LogSegmentParams
            offset = seg.offsets[i]
            length = seg.lengths[i]

            extracted_count = extract_floats_from_bytes(seg.buffer, offset, length, temp_floats)

            if extracted_count > 0:
                # We found the latest valid telemetry entry
                return seg.sequence_ids[i], extracted_count

    return SEQ_NONE, 0


@app_njit()
def minmax_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx, count, out_x, out_y, num_bins):
    if count == 0:
        return 0, 0.0, 0.0

    # Initialize global min/max with the first element
    overall_min = y_2d[start_idx, col_idx]
    overall_max = y_2d[start_idx, col_idx]

    # Fast path: copy if data is sparse
    if count <= num_bins * 4:
        for i in range(count):
            val = y_2d[start_idx + i, col_idx]
            out_x[i] = x_plot[start_idx + i]
            out_y[i] = val
            if val < overall_min:
                overall_min = val
            if val > overall_max:
                overall_max = val
        return count, overall_min, overall_max

    t_min = x_ts[start_idx]
    t_max = x_ts[start_idx + count - 1]
    t_span = t_max - t_min

    if t_span <= 0:
        val_start = y_2d[start_idx, col_idx]
        val_end = y_2d[start_idx + count - 1, col_idx]
        out_x[0], out_y[0] = x_plot[start_idx], val_start
        out_x[1], out_y[1] = x_plot[start_idx + count - 1], val_end
        return 2, min(val_start, val_end), max(val_start, val_end)

    bin_step = t_span / num_bins
    inv_bin_step = 1.0 / bin_step
    bin_target = t_min + bin_step
    out_idx = 0
    chunk_start_rel = 0

    for i in range(1, count):
        curr_idx = start_idx + i
        if x_ts[curr_idx] >= bin_target or i == count - 1:
            chunk_end_rel = i + 1 if i == count - 1 else i

            min_i = start_idx + chunk_start_rel
            max_i = start_idx + chunk_start_rel
            min_val = y_2d[min_i, col_idx]
            max_val = y_2d[max_i, col_idx]

            for j in range(start_idx + chunk_start_rel + 1, start_idx + chunk_end_rel):
                val = y_2d[j, col_idx]
                if val < min_val:
                    min_val, min_i = val, j
                elif val > max_val:
                    max_val, max_i = val, j

            # Update global extents using the chunk's results
            if min_val < overall_min:
                overall_min = min_val
            if max_val > overall_max:
                overall_max = max_val

            # Chronological deduplication logic...
            p1, p4 = start_idx + chunk_start_rel, start_idx + chunk_end_rel - 1
            p2, p3 = (min_i, max_i) if min_i < max_i else (max_i, min_i)

            out_x[out_idx] = x_plot[p1]
            out_y[out_idx] = y_2d[p1, col_idx]
            out_idx += 1
            if p2 != p1:
                out_x[out_idx] = x_plot[p2]
                out_y[out_idx] = y_2d[p2, col_idx]
                out_idx += 1
            if p3 != p2 and p3 != p1:
                out_x[out_idx] = x_plot[p3]
                out_y[out_idx] = y_2d[p3, col_idx]
                out_idx += 1
            if p4 != p3 and p4 != p2 and p4 != p1:
                out_x[out_idx] = x_plot[p4]
                out_y[out_idx] = y_2d[p4, col_idx]
                out_idx += 1

            if i != count - 1:
                chunk_start_rel = i
                bins_passed = (x_ts[curr_idx] - t_min) * inv_bin_step
                bin_target = t_min + (int(bins_passed) + 1) * bin_step

    return out_idx, overall_min, overall_max


@app_njit()
def slice_and_downsample_linear(
    buf: TelemetryBufferBundle,
    col_idx: int,
    out_x: np.ndarray,
    out_y: np.ndarray,
    t_min_s: float,
    t_max_s: float,
    num_bins: int,
):
    """Slices the chronological buffer in time and then downsamples."""
    # 1. Extract from bundle
    x_plot = buf.x_data
    x_ts = buf.x_data_int64
    y_2d = buf.y_data
    start_idx = buf.data_start
    count = buf.data_size

    if count == 0:
        return 0, 0.0, 0.0

    t_min_ns = np.int64(round(t_min_s * 1e9))
    t_max_ns = np.int64(round(t_max_s * 1e9))

    # Early exit
    if x_ts[start_idx + count - 1] < t_min_ns or x_ts[start_idx] > t_max_ns:
        return 0, 0.0, 0.0

    # Fast scan for visibility window
    v_start = -1
    for i in range(count):
        if x_ts[start_idx + i] >= t_min_ns:
            v_start = i
            break
    if v_start == -1:
        return 0, 0.0, 0.0

    v_end = count
    for i in range(v_start, count):
        if x_ts[start_idx + i] > t_max_ns:
            v_end = i
            break

    n_vis = v_end - v_start
    return minmax_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx + v_start, n_vis, out_x, out_y, num_bins)


@app_njit()
def fast_insert_mirrored_buffer(
    x_buf: np.ndarray,
    x_i64_buf: np.ndarray,
    y_buf: np.ndarray,
    head: int,
    size: int,
    batch: TelemetryBatch,
    max_points: int,
) -> tuple[int, int]:
    """
    Inserts new arrays directly into a Mirrored Ring Buffer using compiled C-level loops.
    Returns the updated (head, size).
    """

    new_x = batch.times
    new_x_i64 = batch.times_int64
    new_y = batch.values

    num_new = new_x.size
    if num_new == 0:
        return head, size

    if num_new >= max_points:
        # Massive batch logic - completely overwrite the buffer with the latest data
        idx_start = num_new - max_points

        # Primary buffer
        x_buf[:max_points] = new_x[idx_start:]
        x_i64_buf[:max_points] = new_x_i64[idx_start:]
        y_buf[:max_points, :] = new_y[idx_start:, :]

        # Mirror buffer
        x_buf[max_points : 2 * max_points] = x_buf[:max_points]
        x_i64_buf[max_points : 2 * max_points] = x_i64_buf[:max_points]
        y_buf[max_points : 2 * max_points, :] = y_buf[:max_points, :]

        return 0, max_points

    end_idx = head + num_new
    if end_idx <= max_points:
        # Clean fit (no wrap-around)
        x_buf[head:end_idx] = new_x
        x_i64_buf[head:end_idx] = new_x_i64
        y_buf[head:end_idx, :] = new_y

        # Mirror
        x_buf[head + max_points : end_idx + max_points] = new_x
        x_i64_buf[head + max_points : end_idx + max_points] = new_x_i64
        y_buf[head + max_points : end_idx + max_points, :] = new_y
    else:
        # Wrap-around logic
        overflow = end_idx - max_points
        first_part = num_new - overflow

        # Fill end of primary and mirror
        x_buf[head:max_points] = new_x[:first_part]
        x_buf[head + max_points : 2 * max_points] = new_x[:first_part]

        x_i64_buf[head:max_points] = new_x_i64[:first_part]
        x_i64_buf[head + max_points : 2 * max_points] = new_x_i64[:first_part]

        y_buf[head:max_points, :] = new_y[:first_part, :]
        y_buf[head + max_points : 2 * max_points, :] = new_y[:first_part, :]

        # Fill start of primary and mirror
        x_buf[0:overflow] = new_x[first_part:]
        x_buf[max_points : max_points + overflow] = new_x[first_part:]

        x_i64_buf[0:overflow] = new_x_i64[first_part:]
        x_i64_buf[max_points : max_points + overflow] = new_x_i64[first_part:]

        y_buf[0:overflow, :] = new_y[first_part:, :]
        y_buf[max_points : max_points + overflow, :] = new_y[first_part:, :]

    new_head = end_idx % max_points
    new_size = min(size + num_new, max_points)

    return new_head, new_size
