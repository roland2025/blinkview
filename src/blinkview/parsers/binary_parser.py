# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.configurable import configuration_property, on_config_change, override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.numpy_batch_manager import PooledLogBatch, log_batch
from blinkview.core.time_sync_engine import TimeSyncEngine
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import SyncState, create_default_sync
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

        self.sync_state: SyncState = None

        self.numba_needs_compile = False

        self.logger_batch = None
        self.logger_post = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        self.logger_post = self.logger.child("post", enabled=False)

        self.logger_batch = self.logger.child("batch", enabled=False)

        if self.sync_state is None:
            self.sync_state: SyncState = create_default_sync(self.shared.time_ns())

            print(f"BinaryParser initial sync state: {self.sync_state}")

        factory_build = self.shared.factories.build

        frame_decoder = getattr(self, "frame_decoder", None)
        self.logger.debug(f"frame_decoder config: {frame_decoder}")
        if frame_decoder is not None:
            frame_ctx = SimpleNamespace(
                get_logger=self.logger.child_creator("decoder"),
                device_id=self.local.device_id,
            )
            self._frame_codec = factory_build(
                "frame_decoder", self.frame_decoder, system_ctx=self.shared, local_ctx=frame_ctx
            )

        frame_parser = getattr(self, "frame_parser", None)
        self.logger.debug(f"frame_parser config: {frame_parser}")
        if frame_parser is not None:
            parser_ctx = SimpleNamespace(
                get_logger=self.logger.child_creator("parser"),
                device_id=self.local.device_id,
                sync_state=self.sync_state,
            )
            self._frame_parser = factory_build(
                "frame_parser", frame_parser, system_ctx=self.shared, local_ctx=parser_ctx
            )

        self.numba_needs_compile = False
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
            batch_out_time = 0  # Track when the batch was created
            max_timeout_ns = int(max_timeout * 1e9)  # Nanosecond equivalent for fast math

            _len = len
            _str = str

            codec = self._frame_codec

            f_config = codec.bundle()
            frame_state = FrameState(pool, codec.frame_length_maximum)
            f_state = frame_state.bundle

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

            def batch_acquire() -> PooledLogBatch:
                return pool_create(
                    PooledLogBatch,
                    tuner_out.estimated_capacity,
                    max(tuner_out.estimated_buffer_bytes, codec.frame_length_maximum),
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
                        dummy_in.insert(time_ns(), time_ns(), b"       0.00 V     -0.010 mA \n")
                        dummy_in.insert(time_ns(), time_ns(), b"N1 main reg input          0.00 V     -0.010 mA \n")
                        dummy_in.insert(time_ns(), time_ns(), b"N2 ASI switch")
                        dummy_in.insert(time_ns(), time_ns(), b"              0.00 V")
                        dummy_in.insert(time_ns(), time_ns(), b"     -0.014 mA \n")
                        dummy_in.insert(time_ns(), time_ns(), b"N3 charger input        ")
                        # 3. Trigger the kernel
                        # This will block the thread while LLVM does its work
                        _ = process_batch_kernel(
                            f_config,
                            f_state,
                            dummy_in.bundle,
                            parser_bundle,
                            o_config,
                            dummy_out.bundle,
                        )
                        # print(f"SIGNATURE: {process_batch_kernel.signatures}")
                        self.logger.info("Kernels warmed up and cached.")
                except Exception as e:
                    self.logger.exception("Exception during kernel compilation", e)

                frame_state.reset_batch_trackers()
                frame_state.clear_stitch_state()
                self.numba_needs_compile = False
            # --- [END] WARM UP ---

            def flush():
                nonlocal batch_out, batch_out_time
                if batch_out is not None and batch_out.size > 0:
                    with batch_out:
                        tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=max_timeout)

                        # log_batch(self, batch_out, "OUT")

                        self.distribute(batch_out)
                batch_out = None
                batch_out_time = 0

            while not stop_is_set():
                # 1. Calculate dynamic timeout based on batch age
                if batch_out is not None and batch_out.size > 0:
                    elapsed_ns = time_ns() - batch_out_time
                    remaining_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
                    current_timeout = remaining_timeout
                else:
                    # Will wake up instantly when data arrives OR trigger_shutdown() is called.
                    current_timeout = 120

                batch_in = get(timeout=current_timeout)

                if not batch_in:
                    # No data arrived within the remaining time window
                    flush()
                    continue

                with batch_in:
                    # log_batch(self, batch_in, "IN")

                    batch_size_bytes = batch_in.msg_cursor
                    tuner_out.ensure_burst_capacity(batch_size_bytes)

                    if batch_out and batch_out.buffer_capacity() < tuner_out.estimated_buffer_bytes:
                        flush()

                    # 2. Record creation time when a new batch is acquired
                    if batch_out is None:
                        batch_out = batch_acquire()
                        batch_out_time = time_ns()  # Start the clock

                    speed_in.batch(batch_in)
                    in_bundle = batch_in.bundle
                    in_size = batch_in.size

                    frame_state.reset_batch_trackers()
                    out_is_full = True

                    while f_state.in_idx[0] < in_size or out_is_full:
                        if batch_out and (
                            batch_out.size >= batch_out.capacity
                            or batch_out.msg_cursor >= (batch_out.buffer_capacity() * 0.9)
                        ):
                            flush()

                        # 3. Record creation time during inner loop acquisitions too
                        if batch_out is None:
                            batch_out = batch_acquire()
                            batch_out_time = time_ns()  # Start the clock

                        out_bundle = batch_out.bundle

                        start_time = time_ns()
                        out_is_full = process_batch_kernel(
                            f_config, f_state, in_bundle, parser_bundle, o_config, out_bundle
                        )

                        end_time = time_ns()

                        self.logger_batch.debug(f"{(end_time - start_time) / 1_000_000:.6f}")
                        start_time = time_ns()
                        if parser.post_process(batch_out):
                            parser_bundle = parser.bundle()
                        end_time = time_ns()

                        self.logger_post.debug(f"{(end_time - start_time) / 1_000_000:.6f}")

                        if out_is_full:
                            flush()

                # 4. Final age check: Did the batch expire while we were processing the dribble?
                if batch_out is not None and (time_ns() - batch_out_time) >= max_timeout_ns:
                    flush()

            flush()
        except Exception as e:
            self.logger.exception("run failure", e)
        # Flush any remaining batch on exit
        finally:
            self.numba_needs_compile = False


@ParserFactory.register("serial_default")
class SerialParserThread(BinaryParser):
    __doc__ = "Splitting enabled by default for serial logs, with the split character set to newline (ASCII 10). This is a common configuration for serial log streams, where each log entry is typically separated by a newline character. Users can still customize the split character or disable splitting entirely if their log format differs."


@ParserFactory.register("serial_zephyr_minimal")
@override_property(
    "frame_decoder",
    default={"type": "line_decoder", "frame_length_minimum": 4},
)
@override_property(
    "frame_parser",
    default={"type": "default", "steps": [{"type": "log_level_zephyr_minimal"}]},
)
class ZephyrMinimalParser(SerialParserThread):
    __doc__ = "Default zephyr messages (I: something happened)"


@ParserFactory.register("serial_zephyr_deferred")
@override_property(
    "frame_decoder",
    default={
        "type": "line_decoder",
        "filter_trim_r": True,
        "filter_printable": True,
        "filter_ansi": True,
        "frame_length_minimum": 29,
    },
)
@override_property(
    "frame_parser",
    default={
        "type": "default",
        "steps": [
            {"type": "timestamp_zephyr_uptime_formatted"},
            {"type": "log_level_zephyr"},
            {"type": "module_name_normalizer"},
        ],
    },
)
class ZephyrDeferredParser(SerialParserThread):
    __doc__ = "Zephyr deferred messages"


@ParserFactory.register("serial_zephyr_realtime")
@override_property(
    "frame_decoder",
    default={
        "type": "line_decoder",
        "filter_trim_r": True,
        "filter_printable": True,
        "filter_ansi": True,
        "frame_length_minimum": 29,
    },
)
@override_property(
    "frame_parser",
    default={
        "type": "default",
        "steps": [
            {"type": "timestamp_zephyr_realtime"},
            {"type": "log_level_zephyr"},
            {"type": "module_name_normalizer"},
        ],
    },
)
class ZephyrRealTimeParser(SerialParserThread):
    __doc__ = "Zephyr deferred messages with realtime"


@ParserFactory.register("serial_esp_idf_v1")
@override_property(
    "frame_decoder",
    default={
        "type": "line_decoder",
        "filter_trim_r": True,
        "filter_printable": True,
        "filter_ansi": True,
        "frame_length_minimum": 10,
    },
)
@override_property(
    "frame_parser",
    default={
        "type": "default",
        "steps": [
            {"type": "log_level_idf"},
            {"type": "timestamp_idf_v1"},
            {"type": "module_name_normalizer"},
        ],
    },
)
class EspIdfV1Parser(SerialParserThread):
    __doc__ = "ESP-IDF V1 parser"
