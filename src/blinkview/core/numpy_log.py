# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from threading import Lock

import numpy as np
from numba import njit

from blinkview.core.id_registry import IDRegistry
from blinkview.core.log_row import LogRow
from blinkview.utils.log_level import LogLevel


class LogSegment:
    __slots__ = (
        "segment_seq",
        "capacity",
        "count",
        "_pool",
        # Sliced views (or full slabs)
        "timestamps",
        "levels",
        "modules",
        "devices",
        "seqs",
        "msg_offsets",
        "msg_lens",
        "msg_buffer",
        "msg_cursor",
        # Pool Handles
        "_ts_h",
        "_lvl_h",
        "_mod_h",
        "_dev_h",
        "_seq_h",
        "_off_h",
        "_len_h",
        "_buf_h",
    )

    def __init__(self, pool, segment_seq: int, req_capacity: int, req_buffer_mb: int):
        self._pool = pool
        self.segment_seq = segment_seq
        self.count = 0
        self.msg_cursor = 0

        # Acquire arrays natively from the global pool using element count
        self._ts_h = self._pool.acquire(req_capacity, dtype=np.int64)
        self.timestamps = self._ts_h.array

        # Read true power-of-two capacity
        self.capacity = len(self.timestamps)

        self._lvl_h = self._pool.acquire(self.capacity, dtype=np.uint8)
        self.levels = self._lvl_h.array

        self._mod_h = self._pool.acquire(self.capacity, dtype=np.uint16)
        self.modules = self._mod_h.array

        self._dev_h = self._pool.acquire(self.capacity, dtype=np.uint16)
        self.devices = self._dev_h.array

        self._seq_h = self._pool.acquire(self.capacity, dtype=np.uint64)
        self.seqs = self._seq_h.array

        self._off_h = self._pool.acquire(self.capacity, dtype=np.uint32)
        self.msg_offsets = self._off_h.array

        self._len_h = self._pool.acquire(self.capacity, dtype=np.uint32)
        self.msg_lens = self._len_h.array

        # 1 MB = 1024 * 1024 bytes (elements for uint8)
        self._buf_h = self._pool.acquire(req_buffer_mb * 1024 * 1024, dtype=np.uint8)
        self.msg_buffer = self._buf_h.array

    def clear_and_recycle(self, new_segment_seq: int):
        """O(1) reset to reuse the existing memory slabs for the next rotation."""
        self.segment_seq = new_segment_seq
        self.count = 0
        self.msg_cursor = 0

    def append(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes) -> bool:
        if self.count >= self.capacity:
            return False

        msg_len = len(msg_bytes)
        if self.msg_cursor + msg_len > len(self.msg_buffer):
            return False

        idx = self.count

        self.timestamps[idx] = ts_ns
        self.levels[idx] = level
        self.modules[idx] = module
        self.devices[idx] = device
        self.seqs[idx] = seq

        self.msg_offsets[idx] = self.msg_cursor
        self.msg_lens[idx] = msg_len

        self.msg_buffer[self.msg_cursor : self.msg_cursor + msg_len] = np.frombuffer(msg_bytes, dtype=np.uint8)

        self.msg_cursor += msg_len
        self.count += 1
        return True

    def release(self):
        """Returns arrays to the global pool if this Segment is permanently destroyed."""
        for handle_name in ("_ts_h", "_lvl_h", "_mod_h", "_dev_h", "_seq_h", "_off_h", "_len_h", "_buf_h"):
            h = getattr(self, handle_name)
            if h is not None:
                h.release()
                setattr(self, handle_name, None)


class CircularLogPool:
    def __init__(self, global_pool, max_pieces: int, segment_capacity: int = 100_000, buffer_mb: int = 32):
        self._global_pool = global_pool
        self.max_pieces = max_pieces
        self.segment_capacity = segment_capacity
        self.buffer_mb = buffer_mb

        self.segments: deque[LogSegment] = deque()
        self.segment_counter = 0
        self.active_segment = None

        self._lock = Lock()

        self._rotate_segment()

    def _rotate_segment(self):
        """Creates a new segment OR recycles the oldest one."""
        if len(self.segments) == self.max_pieces:
            # 1. Pop the oldest segment
            recycled_segment = self.segments.popleft()

            # 2. Reset its counters and give it the new sequence ID
            recycled_segment.clear_and_recycle(self.segment_counter)

            # 3. Make it the active segment
            self.active_segment = recycled_segment
            self.segments.append(recycled_segment)
        else:
            # Startup phase: Allocate from the global pool until max_pieces is reached
            new_segment = self._global_pool.create(
                LogSegment, self.segment_counter, self.segment_capacity, self.buffer_mb
            )
            self.segments.append(new_segment)
            self.active_segment = new_segment

        self.segment_counter += 1

    def append(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes):
        with self._lock:
            success = self.active_segment.append(ts_ns, level, module, device, seq, msg_bytes)
            if not success:
                self._rotate_segment()
                self.active_segment.append(ts_ns, level, module, device, seq, msg_bytes)

    def get_ordered_segments(self) -> list[LogSegment]:
        with self._lock:
            return list(self.segments)

    def get_counts(self) -> tuple[int, int]:
        with self._lock:
            current_total = sum(seg.count for seg in self.segments)
            # Use actual power-of-two capacity from the segments
            actual_capacity = self.segments[0].capacity if self.segments else self.segment_capacity
            max_total = self.max_pieces * actual_capacity
            return current_total, max_total

    def release_all(self):
        """Gracefully shutdown and return all memory to the global pool."""
        with self._lock:
            while self.segments:
                seg = self.segments.popleft()
                seg.release()


@njit()
def filter_segment(
    count,
    timestamps,
    levels,
    modules,
    devices,
    seqs,  # Ensure this is here
    target_modules_arr,
    start_seq=-1,  # Ensure this is here
    start_ts=-1,
    end_ts=-1,
    target_level=0xFF,
    target_module=0xFFFF,
    target_device=0xFFFF,
):
    matching_indices = np.empty(count, dtype=np.int64)
    match_count = 0
    use_multi_module = target_modules_arr.size > 0

    for i in range(count):
        # 1. Sequence Check (Fastest exclusion)
        if start_seq != -1 and seqs[i] <= start_seq:
            continue

        # 2. Module Filter
        if use_multi_module:
            found = False
            for m_idx in range(target_modules_arr.size):
                if modules[i] == target_modules_arr[m_idx]:
                    found = True
                    break
            if not found:
                continue
        elif target_module != 0xFFFF and modules[i] != target_module:
            continue

        # 3. Level/Device/Time filters...
        if target_level != 0xFF and levels[i] != target_level:
            continue
        if target_device != 0xFFFF and devices[i] != target_device:
            continue
        if start_ts != -1 and timestamps[i] < start_ts:
            continue
        if end_ts != -1 and timestamps[i] > end_ts:
            continue

        matching_indices[match_count] = i
        match_count += 1

    return matching_indices[:match_count]


def query_pool(id_registry: IDRegistry, pool: CircularLogPool, target_modules: list[int] = None, **filters):
    """Generator that yields populated LogRow objects from all segments."""

    get_level = LogLevel.from_value
    module_from_int = id_registry.module_from_int

    # List to numpy for Numba compatibility
    if target_modules:
        tm_arr = np.array(target_modules, dtype=np.uint16)
    else:
        tm_arr = np.empty(0, dtype=np.uint16)

    # Grab the sequence filter from kwargs
    start_seq = filters.get("start_seq", -1)

    for segment in pool.get_ordered_segments():
        if segment.count == 0:
            continue

        # CRITICAL FIX: Pass 'segment.seqs' and 'start_seq'
        matched_idx = filter_segment(
            segment.count,
            segment.timestamps,
            segment.levels,
            segment.modules,
            segment.devices,
            segment.seqs,  # Wire this in!
            target_modules_arr=tm_arr,
            start_seq=start_seq,  # Wire this in!
            start_ts=filters.get("start_ts", -1),
            end_ts=filters.get("end_ts", -1),
            target_level=filters.get("target_level", 0xFF),
            target_module=filters.get("target_module", 0xFFFF),
            target_device=filters.get("target_device", 0xFFFF),
        )

        for idx in matched_idx:
            offset = segment.msg_offsets[idx]
            length = segment.msg_lens[idx]
            msg = segment.msg_buffer[offset : offset + length].tobytes().decode("utf-8")

            yield LogRow(
                timestamp_ns=segment.timestamps[idx],
                level=get_level(segment.levels[idx]),
                module=module_from_int(segment.modules[idx]),
                message=msg,
                seq=segment.seqs[idx],
            )


@njit(inline="always")
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


@njit(nogil=True)
def extract_telemetry_segment_numba(
    count, timestamps, modules, seqs, msg_offsets, msg_lens, msg_buffer, target_module, start_seq, num_channels
):
    """
    Finds logs, parses bytes to floats, and builds the return arrays in one pass.
    """
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


def fetch_telemetry_arrays(pool: "CircularLogPool", target_module_int: int, start_seq: int, num_channels: int):
    """
    Generator used by the plotter to grab batches of natively formatted NumPy data.
    """
    for segment in pool.get_ordered_segments():
        if segment.count == 0:
            continue

        # Fast exit: Skip entire segment if we've already processed its newest log
        if segment.seqs[segment.count - 1] <= start_seq:
            continue

        # Execute the C-speed extraction
        new_times, new_values, max_seq = extract_telemetry_segment_numba(
            segment.count,
            segment.timestamps,
            segment.modules,
            segment.seqs,
            segment.msg_offsets,
            segment.msg_lens,
            segment.msg_buffer,
            target_module_int,
            start_seq,
            num_channels,
        )

        if len(new_times) > 0:
            yield new_times, new_values, max_seq


@njit()
def peek_segment_channels(count, modules, seqs, msg_offsets, msg_lens, msg_buffer, target_module, start_seq):
    """
    Scans forward to find the first log from target_module that contains floats.
    Returns the number of floats found, or 0 if no numeric logs exist in this segment.
    """
    # Pre-allocate a generous temp buffer (e.g., 256 channels max per log line)
    temp_floats = np.empty(256, dtype=np.float64)

    for i in range(count):
        if modules[i] == target_module and seqs[i] > start_seq:
            offset = msg_offsets[i]
            length = msg_lens[i]

            extracted_count = extract_floats_from_bytes(msg_buffer, offset, length, temp_floats)

            # Keep going if it was just a text-only log!
            if extracted_count > 0:
                return extracted_count

    return 0


def peek_channel_count(pool: "CircularLogPool", target_module_int: int, start_seq: int) -> int:
    """Returns the channel count for the first numeric log after start_seq."""
    for segment in pool.get_ordered_segments():
        if segment.count == 0:
            continue

        if segment.seqs[segment.count - 1] <= start_seq:
            continue

        channels = peek_segment_channels(
            segment.count,
            segment.modules,
            segment.seqs,
            segment.msg_offsets,
            segment.msg_lens,
            segment.msg_buffer,
            target_module_int,
            start_seq,
        )

        if channels > 0:
            return channels

    return 0


@njit(nogil=True)
def format_log_batch_numba(
    indices,
    timestamps,
    levels,
    modules,
    device_ids,
    msg_offsets,
    msg_lens,
    msg_buffer,
    level_params,
    module_params,
    device_params,
    show_ts,
    show_dev,
    show_lvl,
    show_mod,
):
    l_buf, l_off, l_len = level_params
    m_buf, m_off, m_len = module_params
    d_buf, d_off, d_len = device_params

    # PHASE 1: SIZE CALCULATION
    total_size = 0
    for idx in indices:
        row_size = 0
        if show_ts:
            row_size += 13  # "[HH:MM:SS.mmm] "
        if show_dev:
            row_size += d_len[device_ids[idx]] + 1
        if show_lvl:
            row_size += l_len[levels[idx]] + 1
        if show_mod:
            row_size += m_len[modules[idx]] + 2  # "Module: "

        row_size += msg_lens[idx] + 1  # Msg + \n
        total_size += row_size

    out = np.empty(total_size, dtype=np.uint8)
    curr = 0

    for idx in indices:
        first_field = True

        # --- 1. Timestamp ---
        if show_ts:
            ts_ns = timestamps[idx]
            ms = (ts_ns // 1_000_000) % 1000
            sec = (ts_ns // 1_000_000_000) % 60
            mn = (ts_ns // 60_000_000_000) % 60
            hr = (ts_ns // 3_600_000_000_000) % 24

            out[curr + 0] = 48 + (hr // 10)
            out[curr + 1] = 48 + (hr % 10)
            out[curr + 2] = 58
            out[curr + 3] = 48 + (mn // 10)
            out[curr + 4] = 48 + (mn % 10)
            out[curr + 5] = 58
            out[curr + 6] = 48 + (sec // 10)
            out[curr + 7] = 48 + (sec % 10)
            out[curr + 8] = 46
            out[curr + 9] = 48 + (ms // 100)
            out[curr + 10] = 48 + ((ms // 10) % 10)
            out[curr + 11] = 48 + (ms % 10)
            curr += 12
            first_field = False

        # --- 2. Device ---
        if show_dev:
            if not first_field:
                out[curr] = 32
                curr += 1
            d_id = device_ids[idx]
            ln = d_len[d_id]
            off = d_off[d_id]
            out[curr : curr + ln] = d_buf[off : off + ln]
            curr += ln
            first_field = False

        # --- 3. Level ---
        if show_lvl:
            if not first_field:
                out[curr] = 32
                curr += 1
            l_id = levels[idx]
            ln = l_len[l_id]
            off = l_off[l_id]
            out[curr : curr + ln] = l_buf[off : off + ln]
            curr += ln
            first_field = False

        # --- 4. Module ---
        if show_mod:
            if not first_field:
                out[curr] = 32
                curr += 1
            m_id = modules[idx]
            ln = m_len[m_id]
            off = m_off[m_id]
            out[curr : curr + ln] = m_buf[off : off + ln]
            curr += ln
            out[curr] = 58
            curr += 1  # ':'
            first_field = False

        # --- 5. Message ---
        if not first_field:
            out[curr] = 32
            curr += 1
        mo = msg_offsets[idx]
        ml = msg_lens[idx]
        out[curr : curr + ml] = msg_buffer[mo : mo + ml]
        curr += ml

        out[curr] = 10
        curr += 1  # \n

    return out[:curr]
