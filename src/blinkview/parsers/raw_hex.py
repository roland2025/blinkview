# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.hex_dump import create_hex_dump_state
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.hex_dump import hex_dump_line_length, nb_hex_dump
from blinkview.parsers.parser import BaseParser, ParserFactory

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

# Rows a flush is sized for; a burst larger than this is dumped over several batches.
ROWS_PER_BATCH = 1024


@ParserFactory.register("raw_hex")
@override_property("time_sync", hidden=True)
@configuration_property(
    "bytes_per_row",
    title="Bytes per row",
    type="integer",
    enum=[8, 16, 32],
    default=16,
    required=True,
    ui_order=10,
)
@configuration_property(
    "split_on",
    title="Also end a row after byte",
    type="integer",
    minimum=-1,
    maximum=255,
    default=-1,
    required=True,
    ui_order=11,
    description="A byte value that ends the row it is in, so framing shows: 10 for a newline, 0 for COBS. "
    "-1: rows end only at the row width and at the end of each read.",
)
@configuration_property(
    "max_bytes",
    title="Stop after bytes",
    type="integer",
    minimum=0,
    default=1_000_000,
    required=True,
    ui_order=12,
    description="The dump stops after this many bytes and says so in one last row; turn the pipeline off "
    "and on to start again. The rows take 4-5 times the space of the bytes they show. 0: no limit.",
)
@configuration_property(
    "module",
    title="Module",
    type="string",
    default="raw.hex",
    required=True,
    ui_order=13,
    description="Module the rows are logged under, on the device named above.",
)
class RawHexParser(BaseParser):
    __doc__ = """Shows a source's raw bytes as hexdump rows, as they arrived: for working out a device's
framing before a parser is set up for it, or checking what a parser was given.

    00000000: 48 65 6c 6c 6f 20 77 6f 72 6c 64 0a              |Hello world.|

* Each read from the source starts a new row and keeps its receive time.
* Runs next to a source's normal pipeline: both get the same bytes.
* A setup and debugging aid, not something to leave on: it stops after `max_bytes`."""

    bytes_per_row: int
    split_on: int
    max_bytes: int
    module: str

    def run(self):
        get = self.input_queue.get
        time_ns = self.shared.time_ns
        distribute = self.distribute
        stop_is_set = self._stop_event.is_set
        logger = self.logger

        device = self.local.device_id
        device_id = device.id
        module_id = device.get_module(self.module.strip() or "raw.hex").id

        bytes_per_row = self.bytes_per_row
        split_on = self.split_on
        max_bytes = self.max_bytes
        line_len = hex_dump_line_length(bytes_per_row)
        max_timeout = self.delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)

        state = create_hex_dump_state()
        pool_create = self.shared.array_pool.create

        def batch_acquire():
            return pool_create(
                PooledLogBatch,
                ROWS_PER_BATCH,
                ROWS_PER_BATCH * line_len,
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        batch_out = batch_acquire()
        batch_out_time = time_ns()

        def flush():
            nonlocal batch_out, batch_out_time
            if batch_out.size > 0:
                with batch_out:
                    distribute(batch_out)
                batch_out = batch_acquire()
            batch_out_time = time_ns()

        stop_reported = False

        while not stop_is_set():
            if batch_out.size > 0:
                elapsed_ns = time_ns() - batch_out_time
                current_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
            else:
                current_timeout = 120.0

            batch_in = get(timeout=current_timeout)
            if not batch_in:
                flush()
                continue

            with batch_in:
                # After the cap the input is still taken off the queue, and dropped.
                if stop_reported or batch_in.bundle is None or batch_in.size == 0:
                    continue
                try:
                    if batch_out.size == 0:
                        batch_out_time = time_ns()

                    state.in_idx[0] = 0
                    state.in_pos[0] = 0
                    while nb_hex_dump(
                        batch_in.bundle,
                        state,
                        batch_out.bundle,
                        bytes_per_row,
                        split_on,
                        max_bytes,
                        0,
                        module_id,
                        device_id,
                    ):
                        flush()

                    if state.capped[0]:
                        stop_reported = True
                        now = time_ns()
                        message = f"dump stopped after {int(state.stream_offset[0])} bytes (max_bytes)".encode()
                        if not batch_out.insert(now, now, message, module=module_id, device=device_id):
                            flush()
                            batch_out.insert(now, now, message, module=module_id, device=device_id)
                        logger.info("Dump stopped after %s bytes", int(state.stream_offset[0]))
                except Exception as e:
                    logger.exception("Could not dump a batch, skipping it.", exc=e)

            if batch_out.size > 0 and (time_ns() - batch_out_time >= max_timeout_ns):
                flush()

        if batch_out.size > 0:
            flush()
        batch_out.release()

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Compiles nb_hex_dump with batches shaped like run()'s: a raw chunk in (no optional
        columns), levels/modules/devices out."""
        with (
            helper.array_pool.create(PooledLogBatch, 4, 256) as dummy_in,
            helper.array_pool.create(
                PooledLogBatch, 8, 8 * hex_dump_line_length(16), has_levels=True, has_modules=True, has_devices=True
            ) as dummy_out,
        ):
            now = helper.time_ns()
            dummy_in.insert(now, now, b"Hello world\n\x00\xff")
            nb_hex_dump(
                dummy_in.bundle,
                create_hex_dump_state(),
                dummy_out.bundle,
                16,
                10,
                1_000_000,
                0,
                helper.warmup_mod.id,
                helper.warmup_mod.device.id,
            )
