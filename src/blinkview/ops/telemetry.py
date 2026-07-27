# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE, TS_UNSPECIFIED
from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle, TelemetryBatch
from blinkview.core.types.telemetry import TelemetryBufferBundle, TsWindowBundle
from blinkview.ops.segments import nb_fast_find_first_ge, nb_fast_find_first_gt

PLOT_INTERPOLATION_MODE_LINEAR = 0
PLOT_INTERPOLATION_MODE_DISCRETE = 1


@app_njit(inline="always")
def nb_extract_floats_from_bytes(buffer, offset, length, out_array):
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

        nb_is_digit = 48 <= c <= 57
        is_dot = c == 46
        is_minus = c == 45
        is_plus = c == 43

        if nb_is_digit:
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
def nb_extract_telemetry_segment_to_end(
    segment: LogBundle,
    target_module: int,
    start_seq: dtypes.SEQ_TYPE,
    num_channels: int,
    out_times: np.ndarray,  # Full max_points size
    out_times_int64: np.ndarray,
    out_values: np.ndarray,  # Full max_points size
    temp_floats: np.ndarray,
    write_idx: int,  # Starting write position (moves backward)
    effective_mask: np.ndarray,
) -> int:
    """
    Fills arrays from write_idx backwards.
    Returns the updated write_idx.

    effective_mask is the same per-module level threshold ops/segments.py's
    segment_filter/segment_filter_reversed use (row kept iff levels[i] >= effective_mask[modules[i]])
    - lets a caller exclude rows a log-view filter would also exclude, on top of the existing
    exact target_module match. A permissive all-zero mask (dtypes.LEVEL_TYPE(0), the lowest
    threshold) admits every level, reproducing this kernel's original module-only behavior.
    """
    # Metadata for the segment
    count = segment.size[0]
    timestamps = segment.timestamps
    modules = segment.modules
    levels = segment.levels
    seqs = segment.sequences
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
        if seq <= start_seq != SEQ_NONE:
            break

        if modules[i] != target_module:
            continue

        if levels[i] < effective_mask[modules[i]]:
            continue

        # Extract telemetry
        offset = msg_offsets[i]
        length = msg_lens[i]
        extracted_count = nb_extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

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
def nb_extract_telemetry_segment_window_backward(
    segment: LogBundle,
    target_module: int,
    window: TsWindowBundle,
    num_channels: int,
    out_times: np.ndarray,
    out_times_int64: np.ndarray,
    out_values: np.ndarray,
    temp_floats: np.ndarray,
    write_idx: int,  # Starting write position (moves backward)
    effective_mask: np.ndarray,
    capture_edge: bool,
    edge_remaining: int,
):
    """
    Playback-scrub counterpart to nb_extract_telemetry_segment_to_end: instead of stopping at a
    forward-fetch sequence watermark, this is bounded by an arbitrary [window.start_ts,
    window.end_ts] range (same binary-search boundary technique as
    nb_segment_filter_reversed in ops/segments.py) and scans that bounded range newest-to-oldest,
    writing backward from write_idx - same "moves left" convention as
    nb_extract_telemetry_segment_to_end, so a caller can chain multiple segments' calls together
    without a merge/sort step.

    capture_edge/edge_remaining - see fetch_telemetry_window's plus_one docstring
    (core/numpy_log.py). When capture_edge is True and edge_remaining > 0 (the edge row hasn't
    already been captured by a newer segment's call), this same call - after exhausting the
    bounded window scan above - also looks for exactly one more matching row immediately outside
    window.start_ts: the whole segment (newest row first) if this segment's data entirely
    predates window.start_ts, otherwise just the rows below the window's own local lower bound
    within this segment. Writes it at write_idx - 1, contiguous with whatever window rows were
    already written, so the caller never needs a second pass or a separate output slot to stitch
    together afterward.

    Returns (write_idx, edge_remaining) - both threaded into the next (older) segment's call the
    same way write_idx already was before capture_edge existed.

    effective_mask - see nb_extract_telemetry_segment_to_end's docstring.
    """
    count = segment.size[0]
    timestamps = segment.timestamps
    modules = segment.modules
    levels = segment.levels
    msg_offsets = segment.offsets
    msg_lens = segment.lengths
    msg_buffer = segment.buffer

    loop_start = 0
    loop_end = count
    starts_before_segment = False  # True if this whole segment predates window.start_ts

    if window.start_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_ge(timestamps, count, window.start_ts)
        if idx >= count:
            starts_before_segment = True
        if idx > loop_start:
            loop_start = idx

    if window.end_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_gt(timestamps, count, window.end_ts)
        if idx < loop_end:
            loop_end = idx

    if loop_end == 0:
        # Nothing in this segment is <= window.end_ts - it entirely postdates the window, so
        # it's not even an edge candidate (that requires being <= window.end_ts, same as any
        # in-window row).
        return write_idx, edge_remaining

    if loop_start < loop_end:
        for i in range(loop_end - 1, loop_start - 1, -1):
            if write_idx <= 0:
                break

            if modules[i] != target_module:
                continue

            if levels[i] < effective_mask[modules[i]]:
                continue

            offset = msg_offsets[i]
            length = msg_lens[i]
            extracted_count = nb_extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

            if extracted_count >= num_channels:
                write_idx -= 1
                ts_int = timestamps[i]
                out_times[write_idx] = ts_int / 1_000_000_000.0
                out_times_int64[write_idx] = ts_int

                for c in range(num_channels):
                    out_values[write_idx, c] = temp_floats[c]

    if capture_edge and edge_remaining > 0 and write_idx > 0:
        edge_scan_from = count if starts_before_segment else loop_start
        for i in range(edge_scan_from - 1, -1, -1):
            if modules[i] != target_module:
                continue

            if levels[i] < effective_mask[modules[i]]:
                continue

            offset = msg_offsets[i]
            length = msg_lens[i]
            extracted_count = nb_extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

            if extracted_count >= num_channels:
                write_idx -= 1
                ts_int = timestamps[i]
                out_times[write_idx] = ts_int / 1_000_000_000.0
                out_times_int64[write_idx] = ts_int

                for c in range(num_channels):
                    out_values[write_idx, c] = temp_floats[c]

                edge_remaining -= 1
                break

    return write_idx, edge_remaining


@app_njit()
def nb_extract_telemetry_segment_window_forward(
    segment: LogBundle,
    target_module: int,
    window: TsWindowBundle,
    num_channels: int,
    out_times: np.ndarray,
    out_times_int64: np.ndarray,
    out_values: np.ndarray,
    temp_floats: np.ndarray,
    write_idx: int,  # Starting write position (moves forward)
    effective_mask: np.ndarray,
    capture_edge: bool,
    edge_remaining: int,
):
    """
    Forward-direction counterpart to nb_extract_telemetry_segment_window_backward, for the
    "after the scrub anchor" half of a window. Scans the bounded [window.start_ts,
    window.end_ts] range oldest-to-newest, writing forward from write_idx so the combined
    backward+forward output lands pre-sorted ascending with no merge step, mirroring how
    _fetch_history_window (log_viewer.py) pairs a reversed "before" scan with a forward
    "after" scan.

    capture_edge/edge_remaining - mirror image of nb_extract_telemetry_segment_window_backward's
    (see its docstring): once the bounded scan is exhausted, looks for exactly one more matching
    row past window.end_ts - the whole segment (oldest row first) if this segment's data entirely
    postdates window.end_ts, otherwise just the rows at/after the window's own local upper bound
    within this segment - written at write_idx (then incremented), contiguous with whatever
    window rows were already written.

    effective_mask - see nb_extract_telemetry_segment_to_end's docstring.
    """
    count = segment.size[0]
    timestamps = segment.timestamps
    modules = segment.modules
    levels = segment.levels
    msg_offsets = segment.offsets
    msg_lens = segment.lengths
    msg_buffer = segment.buffer
    max_write = out_times.shape[0]

    loop_start = 0
    loop_end = count
    ends_after_segment = False  # True if this whole segment postdates window.end_ts

    if window.start_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_ge(timestamps, count, window.start_ts)
        if idx > loop_start:
            loop_start = idx

    if window.end_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_gt(timestamps, count, window.end_ts)
        if idx <= 0:
            ends_after_segment = True
        if idx < loop_end:
            loop_end = idx

    if loop_start == count:
        # Nothing in this segment is >= window.start_ts - it entirely predates the window, so
        # it's not even an edge candidate (that requires being >= window.start_ts, same as any
        # in-window row).
        return write_idx, edge_remaining

    if loop_start < loop_end:
        for i in range(loop_start, loop_end):
            if write_idx >= max_write:
                break

            if modules[i] != target_module:
                continue

            if levels[i] < effective_mask[modules[i]]:
                continue

            offset = msg_offsets[i]
            length = msg_lens[i]
            extracted_count = nb_extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

            if extracted_count >= num_channels:
                ts_int = timestamps[i]
                out_times[write_idx] = ts_int / 1_000_000_000.0
                out_times_int64[write_idx] = ts_int

                for c in range(num_channels):
                    out_values[write_idx, c] = temp_floats[c]

                write_idx += 1

    if capture_edge and edge_remaining > 0 and write_idx < max_write:
        edge_scan_from = 0 if ends_after_segment else loop_end
        for i in range(edge_scan_from, count):
            if modules[i] != target_module:
                continue

            if levels[i] < effective_mask[modules[i]]:
                continue

            offset = msg_offsets[i]
            length = msg_lens[i]
            extracted_count = nb_extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

            if extracted_count >= num_channels:
                ts_int = timestamps[i]
                out_times[write_idx] = ts_int / 1_000_000_000.0
                out_times_int64[write_idx] = ts_int

                for c in range(num_channels):
                    out_values[write_idx, c] = temp_floats[c]

                write_idx += 1
                edge_remaining -= 1
                break

    return write_idx, edge_remaining


@app_njit()
def nb_peek_segment_channels_backwards(
    seg: LogBundle, target_module: int, start_seq: dtypes.SEQ_TYPE, temp_floats: np.ndarray
):
    """
    Scans BACKWARDS to find the LATEST log from target_module containing telemetry.
    Returns: (found_sequence_id, channel_count)
    """
    sequences = seg.sequences
    modules = seg.modules
    offsets = seg.offsets
    lengths = seg.lengths
    seg_count = seg.size[0]
    for i in range(seg_count - 1, -1, -1):
        # Early exit: if this log is already older than our filter, stop scanning
        if sequences[i] <= start_seq:
            break

        if modules[i] == target_module:
            offset = offsets[i]
            length = lengths[i]

            extracted_count = nb_extract_floats_from_bytes(seg.buffer, offset, length, temp_floats)

            if extracted_count > 0:
                # We found the latest valid telemetry entry
                return sequences[i], extracted_count

    return SEQ_NONE, 0


@app_njit()
def nb_count_module_occurrences_backwards(
    seg: LogBundle, target_module: int, start_seq: dtypes.SEQ_TYPE, limit: int
) -> tuple[int, dtypes.SEQ_TYPE]:
    """
    Counts module entries backwards from a starting sequence.
    Returns: (count_found, earliest_seq_id)
    """
    found = 0
    earliest_seq = start_seq
    seg_count = seg.size[0]

    sequences = seg.sequences
    modules = seg.modules

    # We iterate backwards through the segment
    for i in range(seg_count - 1, -1, -1):
        # We only care about logs older or equal to our starting point
        if sequences[i] > start_seq:
            continue

        if modules[i] == target_module:
            found += 1
            earliest_seq = sequences[i]

            if found >= limit:
                break

    return found, earliest_seq


@app_njit()
def nb_discrete_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx, count, out_x, out_y, num_bins):
    """
    Extracts discrete state changes for the overview plot.
    Iterates in reverse to prioritize the newest data.
    Maintains signature compatibility with nb_minmax_downsample_inplace.
    """
    if count == 0:
        return 0, 0.0, 0.0

    max_out = out_x.shape[0]
    if max_out < 4:
        return 0, 0.0, 0.0

    abs_end = start_idx + count - 1
    abs_start = start_idx

    # Build from the back of the output array
    out_idx = max_out

    curr_y = y_2d[abs_end, col_idx]
    overall_min = curr_y
    overall_max = curr_y

    # 1. Write the absolute newest point
    out_idx -= 1
    out_x[out_idx] = x_plot[abs_end]
    out_y[out_idx] = curr_y

    # Default left-cap is the oldest point in the buffer
    cap_x = x_plot[abs_start]

    # 2. Iterate backwards from second-to-last down to the oldest
    for i in range(abs_end - 1, abs_start - 1, -1):
        test_y = y_2d[i, col_idx]

        # Update hysteresis extents
        if test_y < overall_min:
            overall_min = test_y
        if test_y > overall_max:
            overall_max = test_y

        # Only emit points if the state actually changes
        if test_y != curr_y:
            # Check if we have room for the 2 step points + 1 final cap
            if out_idx < 3:
                cap_x = x_plot[i + 1]
                break

            trans_x = x_plot[i + 1]

            # 2a. Start of the newer state (bottom/top of the vertical jump)
            out_idx -= 1
            out_x[out_idx] = trans_x
            out_y[out_idx] = curr_y

            # 2b. End of the older state (horizontal line extension)
            out_idx -= 1
            out_x[out_idx] = trans_x
            out_y[out_idx] = test_y

            curr_y = test_y

    # 3. Cap off the oldest end
    out_idx -= 1
    out_x[out_idx] = cap_x
    out_y[out_idx] = curr_y

    # 4. Shift the valid data to the front using Numba's optimized slice assignment (memmove)
    n = max_out - out_idx
    out_x[:n] = out_x[out_idx : out_idx + n]
    out_y[:n] = out_y[out_idx : out_idx + n]

    return n, overall_min, overall_max


@app_njit()
def nb_minmax_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx, count, out_x, out_y, num_bins):
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
def nb_slice_and_downsample_linear(
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

    if v_start > 0:
        v_start -= 1

    v_end = count
    for i in range(v_start, count):
        if x_ts[start_idx + i] > t_max_ns:
            v_end = i
            break

    if v_end < count:
        v_end += 1

    n_vis = v_end - v_start
    return nb_minmax_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx + v_start, n_vis, out_x, out_y, num_bins)


@app_njit()
def nb_slice_and_downsample_discrete(
    buf,
    col_idx: int,
    out_x: np.ndarray,
    out_y: np.ndarray,
    t_min_s: float,
    t_max_s: float,
    num_bins: int,
):
    x_plot = buf.x_data
    x_ts = buf.x_data_int64
    y_2d = buf.y_data
    start_idx = buf.data_start
    count = buf.data_size

    if count == 0:
        return 0, 0.0, 0.0

    t_min_ns = np.int64(round(t_min_s * 1e9))
    t_max_ns = np.int64(round(t_max_s * 1e9))

    max_out = out_x.shape[0]
    if max_out < 4:
        return 0, 0.0, 0.0

    # Create a fast Numba array view of the active buffer
    view_x_ts = x_ts[start_idx : start_idx + count]

    # Find the index of the rightmost point <= t_max_ns
    # searchsorted(side='right') gives the index of the first element > t_max_ns
    idx_right = np.searchsorted(view_x_ts, t_max_ns, side="right") - 1

    if idx_right == -1:
        # All data points are > t_max_ns (the data starts strictly to the right of the screen)
        return 0, 0.0, 0.0

    # Find the index of the leftmost point <= t_min_ns
    # This specific point dictates the state entering the left edge of the screen!
    idx_left = np.searchsorted(view_x_ts, t_min_ns, side="right") - 1

    abs_right = start_idx + idx_right
    # If idx_left is -1, it means the data starts after t_min_s, so we lock to the first point
    abs_left = start_idx + max(0, idx_left)

    curr_y = y_2d[abs_right, col_idx]
    overall_min = curr_y
    overall_max = curr_y

    out_idx = max_out

    # 1. Right Cap Edge-to-Edge logic
    out_idx -= 1
    if idx_right < count - 1:
        # If there is data *after* the window, state continues off-screen. Cap perfectly at t_max.
        out_x[out_idx] = t_max_s
    else:
        # We are at the bleeding edge of the live data. Don't extrapolate the future.
        out_x[out_idx] = x_plot[abs_right]
    out_y[out_idx] = curr_y

    # Left Cap Edge-to-Edge logic (calculated here, applied at the end)
    if idx_left != -1:
        # State exists before the window starts. The line should enter perfectly at t_min.
        cap_x = t_min_s
    else:
        # Data starts inside the window. Line starts physically at the first known point.
        cap_x = x_plot[abs_left]

    # 2. Iterate backwards processing the state transitions
    for i in range(abs_right - 1, abs_left - 1, -1):
        test_y = y_2d[i, col_idx]

        # Update hysteresis extents
        if test_y < overall_min:
            overall_min = test_y
        if test_y > overall_max:
            overall_max = test_y

        if test_y != curr_y:
            # We need space for 2 jump points + 1 final cap point
            if out_idx < 3:
                # We hit the point limit. Safely cap exactly where we abandoned the history.
                cap_x = x_plot[i + 1]
                break

            trans_x = x_plot[i + 1]

            # Bottom/Top of the newer state jump
            out_idx -= 1
            out_x[out_idx] = trans_x
            out_y[out_idx] = curr_y

            # End of older state (horizontal extension backwards)
            out_idx -= 1
            out_x[out_idx] = trans_x
            out_y[out_idx] = test_y

            curr_y = test_y

    # 3. Left Cap Implementation
    out_idx -= 1
    out_x[out_idx] = cap_x
    out_y[out_idx] = curr_y

    # 4. Shift valid data to the front for PyQtGraph
    n = max_out - out_idx
    for k in range(n):
        out_x[k] = out_x[out_idx + k]
        out_y[k] = out_y[out_idx + k]

    return n, overall_min, overall_max


@app_njit()
def nb_fast_insert_mirrored_buffer(
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


@app_njit()
def nb_slice_and_downsample(
    buf, col_idx: int, out_x: np.ndarray, out_y: np.ndarray, t_min_s: float, t_max_s: float, num_bins: int, mode: int
):
    if mode == PLOT_INTERPOLATION_MODE_DISCRETE:
        return nb_slice_and_downsample_discrete(buf, col_idx, out_x, out_y, t_min_s, t_max_s, num_bins)

    return nb_slice_and_downsample_linear(buf, col_idx, out_x, out_y, t_min_s, t_max_s, num_bins)


@app_njit()
def nb_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx, count, out_x, out_y, num_bins, mode: int):
    if mode == PLOT_INTERPOLATION_MODE_DISCRETE:
        return nb_discrete_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx, count, out_x, out_y, num_bins)

    return nb_minmax_downsample_inplace(x_plot, x_ts, y_2d, col_idx, start_idx, count, out_x, out_y, num_bins)
