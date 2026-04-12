# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from math import ceil
from types import SimpleNamespace

from blinkview.core.configurable import configuration_property, on_config_change, override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.output import OutputConfig
from blinkview.ops.dispatch import process_batch_kernel
from blinkview.parsers.frame_decoders import FrameDecoder
from blinkview.parsers.frame_parsers import GenericFrameParser
from blinkview.parsers.parser import BaseParser, ParserFactory
from blinkview.parsers.state import FrameState
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner


@configuration_property(
    "frame_decoder",
    type="object",
    required=True,
    ui_order=40,
    _factory="frame_decoder",
    _factory_default="line_decoder",
)
@configuration_property(
    "frame_parser",
    title="Frame parser",
    type="object",
    ui_order=50,
    _factory="frame_parser",
    _factory_default="default",
    required=True,
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

    frame_parser: dict
    frame_decoder: dict

    def __init__(self):
        super().__init__()

        self.parser = None
        self.parse = None  # Localized parse function for speed

        self._frame_codec: FrameDecoder = None
        self._frame_parser: GenericFrameParser = None

        self._assembler = None
        self._assemble = None

        self.numba_needs_compile = True

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        factory_build = self.shared.factories.build

        frame_ctx = SimpleNamespace(
            get_logger=self.logger.child_creator("decoder"),
            device_id=self.local.device_id,
        )
        self.logger.debug(f"frame_decoder config: {self.frame_decoder}")
        self._frame_codec = factory_build(
            "frame_decoder", self.frame_decoder, system_ctx=self.shared, local_ctx=frame_ctx
        )

        parser_ctx = SimpleNamespace(
            get_logger=self.logger.child_creator("parser"),
            device_id=self.local.device_id,
        )
        self.logger.debug(f"frame_parser config: {self.frame_parser}")
        self._frame_parser = factory_build(
            "frame_parser", self.frame_parser, system_ctx=self.shared, local_ctx=parser_ctx
        )

        self.numba_needs_compile = True
        self.thread_needs_restart = True

        return changed

    @on_config_change("name")
    def name_changed(self, name, old):
        self.logger.info(f"Device name changed from '{old}' to '{name}'")
        # If the device name changes, we may want to update the device identity in the assembler
        dev_id: DeviceIdentity = self.local.device_id
        dev_id.name = name

    def run(self):
        try:
            self.logger.info("Starting parser thread")
            get = self.input_queue.get

            max_timeout = self.delay / 1000.0  # Convert milliseconds to seconds

            time_ns = self.shared.time_ns

            pool = self.shared.array_pool
            pool_create = pool.create

            batch_out = None

            _len = len
            _str = str

            codec = self._frame_codec

            f_config = codec.bundle()
            frame_state = FrameState(pool, ceil(codec.frame_length_maximum / 1024))
            f_state = frame_state.bundle()

            o_config = OutputConfig(compact_buffer=getattr(self, "compact_buffer", True))

            parser = self._frame_parser
            parser_bundle = parser.bundle()

            stop_is_set = self._stop_event.is_set

            # --- Auto-Tuning Trackers ---
            speed_in = Speedometer(logger=self.logger.child("stats_in"))
            speed_out = Speedometer(logger=self.logger.child("stats_out"))

            tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

            logger_in = self.logger.child("batch_in")
            logger_out = self.logger.child("batch_out")

            def batch_acquire():
                return pool_create(
                    PooledLogBatch,
                    tuner_out.estimated_capacity,
                    tuner_out.estimated_buffer_kb,
                    has_levels=True,
                    has_modules=True,
                    has_devices=True,
                )

            # --- [START] WARM UP THE NUMBA KERNEL ---
            if self.numba_needs_compile:
                try:
                    self.logger.info("Pre-compiling Numba kernels (this may take a few seconds)...")

                    def batch_acquire_input():
                        return pool_create(PooledLogBatch, 128, 4)  # 128 items, 4 kB buffer for the dummy batch

                    # 1. Create a dummy input batch
                    # We need a small buffer and at least one 'row' entry
                    with (
                        batch_acquire_input() as dummy_in,
                        batch_acquire() as dummy_out,
                    ):
                        dummy_in.insert(time_ns(), b"       0.00 V     -0.010 mA \n")
                        dummy_in.insert(time_ns(), b"N1 main reg input          0.00 V     -0.010 mA \n")
                        dummy_in.insert(time_ns(), b"N2 ASI switch")
                        dummy_in.insert(time_ns(), b"              0.00 V")
                        dummy_in.insert(time_ns(), b"     -0.014 mA \n")
                        dummy_in.insert(time_ns(), b"N3 charger input        ")
                        # 3. Trigger the kernel
                        # This will block the thread while LLVM does its work
                        _, _, _ = process_batch_kernel(
                            f_config,
                            f_state,
                            dummy_in.bundle(),
                            parser_bundle,
                            o_config,
                            dummy_out.bundle(),
                        )

                        self.logger.info("Kernels warmed up and cached.")
                except Exception as e:
                    self.logger.exception("Exception during kernel compilation", e)

                frame_state.reset_batch_trackers()
                frame_state.clear_stitch_state()
                self.numba_needs_compile = False
            # --- [END] WARM UP ---

            def flush():
                nonlocal batch_out
                if batch_out is not None and batch_out.size > 0:
                    with batch_out:
                        # 3. Update tuner_out with the TARGET window
                        # This projects sizing for the next batch correctly
                        tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=max_timeout)
                        self.distribute(batch_out)
                batch_out = None

            while not stop_is_set():
                batch_in = get(timeout=max_timeout)

                if not batch_in:
                    # No data
                    flush()
                    # last_bps_time = time_ns()
                    continue

                with batch_in:
                    # logger_in.debug(str(batch_in))

                    # print(f"[parser] batch_in={batch_in}")
                    batch_size_bytes = batch_in.msg_cursor

                    # =====================================================================
                    # BURST SAFETY: Ensure our planned allocation can at least hold the
                    # payload we are holding in our hands right now.
                    # =====================================================================
                    tuner_out.ensure_burst_capacity(batch_size_bytes)

                    # If we have an active batch that is too small for our new burst estimate,
                    # flush it immediately to force a massive batch allocation.
                    if batch_out and batch_out.buffer_len() < (tuner_out.estimated_buffer_kb * 1024):
                        flush()

                    # TODO: check if we have enough room in current batch?

                    # --- Lazy Allocation ---
                    if batch_out is None:
                        batch_out = batch_acquire()

                    # --- Throughput Calculation ---
                    speed_in.batch(batch_in)

                    # ... process in_batch and assemble your PooledLogBatch ...
                    # 1. Bundle up our SoA (Structure of Arrays) views
                    in_bundle = batch_in.bundle()
                    in_size = batch_in.size

                    # Initialization for the state machine
                    frame_state.reset_batch_trackers()
                    out_is_full = True

                    # Continue looping until the input batch is fully consumed AND
                    # all resulting complete frames are extracted from the f_buf
                    while f_state.in_idx[0] < in_size or out_is_full:
                        if batch_out and (
                            batch_out.size >= batch_out.capacity
                            or batch_out.msg_cursor >= (batch_out.buffer_len() * 0.9)
                        ):
                            flush()

                        if batch_out is None:
                            batch_out = batch_acquire()

                        out_bundle = batch_out.bundle()

                        # 2. Run the Kernel
                        new_size, new_cursor, out_is_full = process_batch_kernel(
                            f_config, f_state, in_bundle, parser_bundle, o_config, out_bundle
                        )

                        # 3. Sync state
                        batch_out.size = new_size
                        batch_out.msg_cursor = new_cursor

                        if parser.post_process(batch_out):
                            parser_bundle = parser.bundle()

                        # 5. Flush immediately if the kernel paused because the output is full
                        if out_is_full:
                            flush()

                # print(f"[parser] batch_out={batch_out}")
                # logger_out.debug(str(batch_out))
                # for out in batch_out:
                #     logger_out.trace(str(out))

                # --- Check for Flush ---
                if batch_out.size >= batch_out.capacity or batch_out.msg_cursor >= (batch_out.buffer_len() * 0.9):
                    flush()

            flush()
        except Exception as e:
            self.logger.exception("run failure", e)
        # Flush any remaining batch on exit
        finally:
            self.numba_needs_compile = False


@ParserFactory.register("serial_default")
@override_property("frame_decoder", default={"type": "line_decoder"})
# @override_property("_factory_default", default={})
# @override_property("transform", default={"type": "default", "steps": [{"type": "ansi_filter"}]})
# @override_property("assembler", default={"type": "default", "message_index": 0})
class SerialParserThread(BinaryParser):
    __doc__ = "Splitting enabled by default for serial logs, with the split character set to newline (ASCII 10). This is a common configuration for serial log streams, where each log entry is typically separated by a newline character. Users can still customize the split character or disable splitting entirely if their log format differs."
