# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np
from numba import njit

from blinkview.core.configurable import configuration_property, on_config_change, override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.parser import BaseParser, ParserFactory


class FrameState:
    def __init__(self, pool, size_kb=4):
        self._pool_handle = pool.acquire(size_kb * 1024, dtype=np.uint8)

        self.buffer = self._pool_handle.array
        self.write_offset = 0
        self.in_frame = False

    def release(self):
        self._pool_handle.release()


# 2. The Input Context (The "Now")
class InputParams(NamedTuple):
    ts_in: int  # Timestamp for this batch
    split_char: int  # Delimiter (e.g., ord('\n'))
    def_lvl: int  # Default Level
    def_mod: int  # Default Module ID
    def_dev: int  # Default Device ID


# 3. The Output Destination (The PooledLogBatch views)
class LogOutput(NamedTuple):
    ts: np.ndarray  # int64
    off: np.ndarray  # uint32
    lengths: np.ndarray  # uint32
    buf: np.ndarray  # uint8
    # Optional columns (Pass empty arrays if not used)
    lvl: np.ndarray  # uint8
    mod: np.ndarray  # uint16
    dev: np.ndarray  # uint16
    seq: np.ndarray  # uint64
    # Status flags
    has_lvl: bool
    has_mod: bool
    has_dev: bool
    has_seq: bool


@njit
def decode_newline_frame(f_buf, start, end, out_buf, out_cursor):
    """Type 0: Just copies the raw frame bytes."""
    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    for i in range(start, end):
        if cursor < buf_cap:
            out_buf[cursor] = f_buf[i]
            cursor += 1
    return cursor


@njit
def decode_cobs_frame(f_buf, start, end, out_buf, out_cursor):
    """Type 1: Decodes COBS pointers to reconstruct raw payload."""
    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    if end - start < 2:
        return cursor

    read_idx = start
    while read_idx < end:
        code = f_buf[read_idx]
        if code == 0:
            break
        read_idx += 1

        for i in range(1, code):
            if read_idx >= end:
                break
            if cursor < buf_cap:
                out_buf[cursor] = f_buf[read_idx]
                cursor += 1
            read_idx += 1

        if code < 0xFF and read_idx < end:
            if cursor < buf_cap:
                out_buf[cursor] = 0x00
                cursor += 1

    return cursor


@njit
def decode_slip_frame(f_buf, start, end, out_buf, out_cursor):
    """Type 2: Unescapes SLIP byte sequences."""
    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    i = start
    while i < end:
        val = f_buf[i]
        if val == 0xDB:
            i += 1
            if i < end:
                esc = f_buf[i]
                if esc == 0xDC:
                    val = 0xC0
                elif esc == 0xDD:
                    val = 0xDB
                else:
                    val = esc

        if cursor < buf_cap:
            out_buf[cursor] = val
            cursor += 1
        i += 1

    return cursor


@njit
def filter_printable_inplace(out_buf, start_cursor, end_cursor):
    """
    Sweeps through the decoded payload and drops non-printable characters.
    Because it writes to the same buffer it reads from, it operates
    with zero memory allocation overhead.
    """
    write_cursor = start_cursor
    for i in range(start_cursor, end_cursor):
        val = out_buf[i]
        if 32 <= val <= 126:
            out_buf[write_cursor] = val
            write_cursor += 1

    return write_cursor


@njit
def filter_ansi_inplace(out_buf, start_cursor, end_cursor):
    """
    Sweeps through the decoded payload and drops ANSI escape sequences
    (specifically standard CSI sequences like `ESC [ ... m`).
    Operates in-place with zero memory allocation.
    """
    write_cursor = start_cursor
    state = 0  # 0: normal text, 1: seen ESC, 2: inside CSI sequence

    for i in range(start_cursor, end_cursor):
        val = out_buf[i]

        if state == 0:
            if val == 27:  # 0x1B (ESC)
                state = 1
            else:
                out_buf[write_cursor] = val
                write_cursor += 1

        elif state == 1:
            if val == 91:  # 0x5B ('[')
                state = 2
            else:
                # Not a CSI sequence. Drop the orphan ESC, keep this byte, and reset.
                state = 0
                out_buf[write_cursor] = val
                write_cursor += 1

        elif state == 2:
            # CSI sequences end with a byte in the range 0x40-0x7E (64-126)
            # Intermediate/parameter bytes are 0x20-0x3F, which we just skip.
            if 64 <= val <= 126:
                state = 0  # Sequence finished

    return write_cursor


@njit
def shift_frame_buffer(f_buf, read_ptr, write_ptr):
    residue_len = write_ptr - read_ptr
    if residue_len > 0 and read_ptr > 0:
        for n in range(residue_len):
            f_buf[n] = f_buf[read_ptr + n]
        return residue_len
    elif read_ptr > 0:
        return 0
    return write_ptr


@njit
def process_batch_kernel(
    frame_decode_func,
    frame_delimiter,
    frame_length_min,
    frame_offset,
    frame_in,
    frame_buffer,
    in_b,
    out_b,
    out_idx,
    out_cursor,
    filter_printable,
    filter_ansi,
):
    curr_write = frame_offset
    curr_in_frame = frame_in
    f_buf = frame_buffer
    in_size = in_b.size

    curr_out_idx = out_idx
    curr_out_cursor = out_cursor
    out_cap = out_b.timestamps.shape[0]

    for i in range(in_size):
        ts_in = in_b.timestamps[i]
        in_off = in_b.offsets[i]
        in_len = in_b.lengths[i]

        for j in range(in_len):
            if curr_write < f_buf.shape[0]:
                f_buf[curr_write] = in_b.buffer[in_off + j]
                curr_write += 1

        read_ptr = 0
        for k in range(curr_write):
            if f_buf[k] == frame_delimiter:
                if curr_in_frame:
                    if k > read_ptr:
                        if curr_out_idx >= out_cap:
                            break

                        out_b.timestamps[curr_out_idx] = ts_in
                        out_b.offsets[curr_out_idx] = curr_out_cursor

                        # DECODE: Execute the passed-in function directly!
                        raw_cursor = frame_decode_func(f_buf, read_ptr, k, out_b.buffer, curr_out_cursor)

                        # FILTER
                        final_cursor = raw_cursor

                        if filter_ansi:
                            final_cursor = filter_ansi_inplace(out_b.buffer, curr_out_cursor, final_cursor)

                        if filter_printable:
                            final_cursor = filter_printable_inplace(out_b.buffer, curr_out_cursor, final_cursor)

                        # LENGTH VALIDATION
                        payload_length = final_cursor - curr_out_cursor

                        if payload_length >= frame_length_min:
                            out_b.lengths[curr_out_idx] = payload_length
                            curr_out_cursor = final_cursor

                            if out_b.has_levels and in_b.has_levels:
                                out_b.levels[curr_out_idx] = in_b.levels[i]

                            curr_out_idx += 1

                    curr_in_frame = False
                read_ptr = k + 1
            else:
                if not curr_in_frame:
                    curr_in_frame = True
                    read_ptr = k

        curr_write = shift_frame_buffer(f_buf, read_ptr, curr_write)

    return curr_out_idx, curr_out_cursor, curr_write, curr_in_frame


@configuration_property(
    "split",
    type="object",
    ui_order=10,
    # --- Explicitly define the fields inside this object ---
    properties={
        "char": {
            "type": "integer",
            "title": "Split Character (ASCII)",
            "minimum": 0,
            "maximum": 255,
            "default": 10,  # Default to newline character
        }
    },
    description="Settings for splitting raw byte streams into packets.",
)
@configuration_property(
    "printable",
    type="object",
    ui_order=20,
    _factory="pipeline_printable",
    _factory_default="bytes_translate",
    description="Filters non-printable characters before decoding.",
)
@configuration_property(
    "transform",
    type="object",
    ui_order=40,
    _factory="pipeline_transform",
    _factory_default="default",
    description="Data transformation steps to apply to each log entry after decoding.",
)
@configuration_property(
    "assembler",
    title="Line parser",
    type="object",
    ui_order=50,
    _factory="pipeline_assembler",
    _factory_default="default",
    description="Assembles transformed log entries into final LogRow objects, potentially using timestamp and device identity.",
)
@configuration_property(
    "ignore_invalid",
    description="On error, ignore the line instead of writing an error. This can be useful for noisy logs with occasional malformed lines.",
    type="boolean",
    default=False,
    ui_order=5,
)
@ParserFactory.register("default")
class BinaryParser(BaseParser):
    __doc__ = """The default pipeline, designed for maximum flexibility and configurability.

* Supports optional splitting of raw byte streams
* filtering of non-printable characters
* decoding of bytes to strings
* arbitrary transformations
* and final assembly into LogRow objects. 

Each stage is configurable via the factory system, allowing users to mix and match different implementations or skip stages entirely for maximum performance when certain features are not needed."""

    ignore_invalid: bool
    split: dict
    printable: dict
    decoder: dict
    transformer: dict
    assembler: dict

    def __init__(self):
        super().__init__()

        self.parser = None
        self.parse = None  # Localized parse function for speed

        self._split_char = None

        self._printable = None
        self._print = None

        self._decoder = None
        self._decode = None

        self._transformer = None
        self._transform = None

        self._assembler = None
        self._assemble = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        factory_build = self.shared.factories.build

        split_cfg = getattr(self, "split", None)
        if split_cfg is not None:
            self._split_char = int(split_cfg.get("char", 10))
        else:
            self._split_char = None

        printable_cfg = getattr(self, "printable", None)
        if printable_cfg:
            self._printable = factory_build("pipeline_printable", printable_cfg, self.shared)
            self._print = self._printable.process
        else:
            self._print = None
            self._printable = None

        decoder_cfg = getattr(self, "decode", None)
        if decoder_cfg:
            self._decoder = factory_build("pipeline_decode", decoder_cfg, self.shared)
            self._decode = self._decoder.process
        else:
            self._decode = None
            self._decoder = None

        transformer_cfg = getattr(self, "transform", None)
        if transformer_cfg:
            self.logger.debug(f"transform config: {transformer_cfg}")
            self._transformer = factory_build("pipeline_transform", transformer_cfg, system_ctx=self.shared)
            self._transform = self._transformer.process
        else:
            self._transform = None
            self._transformer = None

        assembler_cfg = getattr(self, "assembler", {})
        if assembler_cfg:
            self._assembler = factory_build("pipeline_assembler", assembler_cfg, system_ctx=self.shared)
            self._assemble = self._assembler.process
        else:
            self._assembler = None
            self._assemble = None

        self.thread_needs_restart = True

        return changed

    @on_config_change("name")
    def name_changed(self, name, old):
        self.logger.info(f"Device name changed from '{old}' to '{name}'")
        # If the device name changes, we may want to update the device identity in the assembler
        dev_id: DeviceIdentity = self.local.device_id
        dev_id.name = name

    def run(self):
        self.logger.info("Starting parser thread")
        get = self.input_queue.get

        max_batch = self.max_batch
        max_timeout = self.delay / 1000.0  # Convert milliseconds to seconds

        time_ns = self.shared.time_ns

        log_error_row_on_invalid = not self.ignore_invalid

        device_identity = self.local.device_id

        pool = self.shared.array_pool
        pool_create = pool.create

        # pool_acquire = self.shared.np_pool.get("LogRows", has_levels=True, has_modules=True, has_devices=True).acquire

        # Pre-allocate our 'Resident' buffer from the pool (e.g. 64KB uint8)
        # This persists across loops to handle residues
        frame_state = FrameState(pool)

        batch_out = None

        error = self.logger.error

        _split_char = self._split_char
        _print = self._print
        _decode = self._decode
        _transform = self._transform
        _assemble = self._assemble
        _len = len
        _str = str

        frame_length_min = 40
        frame_type = 0

        filter_printable = True
        filter_ansi = True

        if frame_type == 1:
            frame_decode_func = decode_cobs_frame
            frame_delimiter = 0x00
        elif frame_type == 2:
            frame_decode_func = decode_slip_frame
            frame_delimiter = 0xC0
        else:
            frame_decode_func = decode_newline_frame
            frame_delimiter = _split_char

        if _assemble is None:
            module_log = device_identity.get_module("log")

        module_unknown = None

        stop_is_set = self._stop_event.is_set

        # --- Auto-Tuning Trackers ---
        bytes_accumulated = 0
        msgs_accumulated = 0
        last_bps_time = time_ns()
        ONE_SECOND_NS = 1_000_000_000
        SAFETY_FACTOR = 1.5

        logger_in = self.logger.child("batch_in")
        logger_out = self.logger.child("batch_out")
        logger_stats = self.logger.child("stats")

        # Safe defaults for the very first batch before we have metrics
        estimated_buffer_kb = default_buffer_kb = 4
        estimated_capacity = default_capacity = default_buffer_kb * 1024 / 32  # 32 chars per msg

        def batch_acquire():
            return pool_create(
                PooledLogBatch,
                estimated_capacity,
                estimated_buffer_kb,
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        def flush():
            nonlocal batch_out
            if batch_out is not None and batch_out.size > 0:
                with batch_out:
                    # self.distribute(batch_out)
                    pass
            batch_out = None

        while not stop_is_set():
            batch_in = get(timeout=max_timeout)

            if not batch_in:
                # No data
                flush()
                # last_bps_time = time_ns()
                continue

            with batch_in:
                logger_in.debug(str(batch_in))
                batch_size_bytes = batch_in.msg_cursor

                # =====================================================================
                # BURST SAFETY: Ensure our planned allocation can at least hold the
                # payload we are holding in our hands right now.
                # =====================================================================
                required_kb = (batch_size_bytes // 1024) + 1
                if required_kb > estimated_buffer_kb:
                    estimated_buffer_kb = int(required_kb * SAFETY_FACTOR)
                    estimated_capacity = int((estimated_buffer_kb * 1024) / 32)

                # If we have an active batch that is too small for our new burst estimate,
                # flush it immediately to force a massive batch allocation.
                if batch_out and batch_out.buffer_len() < (estimated_buffer_kb * 1024):
                    flush()

                # TODO: check if we have enough room in current batch?

                # --- Lazy Allocation ---
                if batch_out is None:
                    batch_out = batch_acquire()

                # --- Throughput Calculation ---
                bytes_accumulated += batch_size_bytes
                current_time = time_ns()
                if current_time - last_bps_time >= ONE_SECOND_NS:
                    elapsed_sec = (current_time - last_bps_time) / ONE_SECOND_NS

                    # Throughput per second
                    bytes_per_sec = bytes_accumulated / elapsed_sec
                    msgs_per_sec = msgs_accumulated / elapsed_sec

                    # Auto-Tune: How much do we need for ONE `max_timeout` window?
                    bytes_needed = bytes_per_sec * max_timeout * SAFETY_FACTOR
                    # msgs_needed = msgs_per_sec * max_timeout
                    msgs_needed = bytes_needed / 32

                    # Apply safety factor and convert to KB (ensure minimums so we don't request 0)
                    estimated_buffer_kb = max(default_buffer_kb, int((bytes_needed) / 1024))
                    estimated_capacity = max(default_capacity, int(msgs_needed))

                    mb_per_sec = bytes_per_sec / (1024 * 1024)
                    logger_stats.debug(
                        f"speed={mb_per_sec:.2f} MB/s tuned_rows={estimated_capacity} tuned_buffer_kb={estimated_buffer_kb}"
                    )

                    # Reset trackers
                    bytes_accumulated = 0
                    msgs_accumulated = 0
                    last_bps_time = current_time

                # ... process in_batch and assemble your PooledLogBatch ...
                # 1. Bundle up our SoA (Structure of Arrays) views
                in_bundle = batch_in.bundle()
                out_bundle = batch_out.bundle()

                # 2. Process the ENTIRE batch in one JIT-compiled sweep
                new_size, new_cursor, new_offset, new_in_frame = process_batch_kernel(
                    frame_decode_func,
                    frame_delimiter,
                    frame_length_min,
                    frame_state.write_offset,
                    frame_state.in_frame,
                    frame_state.buffer,
                    in_bundle,
                    out_bundle,
                    batch_out.size,
                    batch_out.msg_cursor,
                    filter_printable,
                    filter_ansi,
                )

                # 3. Update the persistent FrameState
                frame_state.write_offset = new_offset
                frame_state.in_frame = new_in_frame

                # 4. Sync the Batch state
                batch_out.size = new_size
                batch_out.msg_cursor = new_cursor

            logger_out.debug(str(batch_out))
            for out in batch_out:
                logger_out.trace(str(out))

            # --- Check for Flush ---
            if batch_out.size >= batch_out.capacity or batch_out.msg_cursor >= (batch_out.buffer_len() * 0.9):
                flush()

        # Flush any remaining batch on exit
        flush()


@ParserFactory.register("serial_default")
@override_property("split", default={})  # Default to newline character for splitting
@override_property("printable", default={})
# @override_property("decode", default={})
@override_property("transform", default={"type": "default", "steps": [{"type": "ansi_filter"}]})
@override_property("assembler", default={"type": "default", "message_index": 0})
class SerialParserThread(BinaryParser):
    __doc__ = "Splitting enabled by default for serial logs, with the split character set to newline (ASCII 10). This is a common configuration for serial log streams, where each log entry is typically separated by a newline character. Users can still customize the split character or disable splitting entirely if their log format differs."
