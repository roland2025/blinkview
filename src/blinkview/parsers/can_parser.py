# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

from blinkview.core.configurable import configuration_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.parser import BaseParser, ParserFactory
from blinkview.utils.log_level import LogLevel
from blinkview.utils.paths import resolve_config_path
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

if TYPE_CHECKING:
    from blinkview.core.device_identity import DeviceIdentity


@dataclass(slots=True)
class DBCMsgInfo:
    name: str
    decode: Callable
    signal_map: dict[str, int]


@ParserFactory.register("cantools")
@configuration_property(
    "dbc_file",
    type="string",
    required=True,
    ui_type="file",
    ui_file_filter="DBC Files (*.dbc);;All Files (*)",
    description="Absolute or relative path to the .dbc database file.",
)
@configuration_property(
    "strict",
    type="boolean",
    default=False,
    required=True,
    ui_order=10,
    description="If true, logs each unknown CAN ID as an ERROR row instead of an INFO UNMAPPED row. "
    "Overrides ignore_unknown.",
)
@configuration_property(
    "ignore_unknown",
    type="boolean",
    default=False,
    required=True,
    description="If true, silently ignores messages not defined in the DBC file by dropping them.",
)
class CantoolsParser(BaseParser):
    __doc__ = """Decodes raw CAN frames into physical values using a DBC file.

* Reads batches of raw CAN data from the upstream CANReader.
* Maps hardware addresses (ext_u32_1) to DBC definitions.
* Decodes the payload and formats it into readable log messages.
"""

    dbc_file: str
    strict: bool
    ignore_unknown: bool

    def __init__(self):
        super().__init__()
        self.db = None
        self._msg_info_map = {}

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        if getattr(self, "dbc_file", None):
            try:
                self.logger.info("Loading DBC file: %s", self.dbc_file)
                # noinspection PyPackageRequirements
                from cantools import database

                self.db = database.load_file(resolve_config_path(self.dbc_file))
            except Exception as e:
                self.logger.error("Failed to load DBC file: %s", self.dbc_file, exc=e)
                self.db = None

        # --- Pre-bake Logic ---
        # Build a new map and swap it in, so a running run() never sees a half-filled one
        msg_info_map = {}

        if self.db:
            get_device_module = self.local.device_id.get_module

            for msg in self.db.messages:
                # Pre-calculate the module IDs exclusively for this message's signals
                local_signal_map = {signal.name: get_device_module(signal.name).id for signal in msg.signals}

                msg_info_map[msg.frame_id] = DBCMsgInfo(name=msg.name, decode=msg.decode, signal_map=local_signal_map)

        self._msg_info_map = msg_info_map

        return changed

    def run(self):
        try:
            self.logger.info("Starting Cantools parser thread")
            get = self.input_queue.get

            # A row waits at most `delay` before its batch is distributed
            max_timeout = self.delay / 1000.0
            max_timeout_ns = int(max_timeout * 1e9)
            time_ns = self.shared.time_ns
            stop_is_set = self._stop_event.is_set

            device: "DeviceIdentity" = self.local.device_id

            device_id_int = device.id

            get_device_module = device.get_module

            module_unknown_id_int = get_device_module("unknown").id

            # CAN IDs whose decode failure was already logged, so a DBC/bus mismatch logs once per ID
            decode_failure_logged = set()

            log_level_error_int = LogLevel.ERROR.value
            log_level_info_int = LogLevel.INFO.value

            pool_create = self.shared.array_pool.create

            # --- Auto-Tuning Trackers ---
            speed_in = Speedometer(logger=self.logger.stats_child("stats_in"))
            speed_out = Speedometer(logger=self.logger.stats_child("stats_out"))
            tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.stats_child("tuner_out"))

            def batch_acquire() -> PooledLogBatch:
                return pool_create(
                    PooledLogBatch,
                    tuner_out.estimated_capacity,
                    tuner_out.estimated_buffer_bytes,
                    has_levels=True,
                    has_modules=True,
                    has_devices=True,
                )

            batch_out = None
            batch_out_time = 0  # When the first row went into batch_out, 0 while it is empty

            def flush():
                nonlocal batch_out, batch_out_time
                if batch_out is not None and batch_out.size > 0:
                    with batch_out:
                        tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=max_timeout)
                        self.distribute(batch_out)
                batch_out = None
                batch_out_time = 0

            def insert_row(ts, out_bytes, level, mod_id):
                nonlocal batch_out, batch_out_time
                if not batch_out.insert(ts, ts, out_bytes, level, mod_id, device_id_int):
                    flush()
                    batch_out = batch_acquire()
                    if not batch_out.insert(ts, ts, out_bytes, level, mod_id, device_id_int):
                        self.logger.warn("Dropped a %d-byte row that does not fit an empty batch", len(out_bytes))
                        return
                if not batch_out_time:
                    batch_out_time = time_ns()  # Start the clock

            while not stop_is_set():
                try:
                    if batch_out_time:
                        # Wait only for what is left of the pending batch's delay window
                        timeout = max(0.0, max_timeout - (time_ns() - batch_out_time) / 1e9)
                    else:
                        # Nothing pending; get() wakes when data arrives or the parser is stopped
                        timeout = 120

                    batch_in = get(timeout=timeout)

                    if not batch_in:
                        flush()
                        continue

                    with batch_in:
                        speed_in.batch(batch_in)

                        # Re-read per batch so a live reconfigure takes effect
                        info_map_get = self._msg_info_map.get
                        strict = self.strict
                        ignore_unknown = self.ignore_unknown

                        # Estimate burst size (assume decoded string takes ~80-120 bytes max)
                        estimated_out_bytes = batch_in.size * 128
                        tuner_out.ensure_burst_capacity(estimated_out_bytes)

                        if batch_out and batch_out.buffer_capacity() < tuner_out.estimated_buffer_bytes:
                            flush()

                        if batch_out is None:
                            batch_out = batch_acquire()

                        # 1. Localize input array references
                        b_in = batch_in.bundle
                        size = batch_in.size
                        timestamps = memoryview(b_in.timestamps)
                        offsets = memoryview(b_in.offsets)
                        lengths = memoryview(b_in.lengths)
                        buf = memoryview(b_in.buffer)

                        # Extension column coming from CANReader
                        can_ids = memoryview(b_in.ext_u32_1)

                        try:
                            # 2. Extract, Decode, and Insert Loop
                            for i in range(size):
                                can_id = can_ids[i]
                                off = offsets[i]
                                data = buf[off : off + lengths[i]].tobytes()
                                ts = timestamps[i]

                                msg_info: Optional[DBCMsgInfo] = info_map_get(can_id)

                                if msg_info is not None:
                                    try:
                                        decoded = msg_info.decode(data)
                                    except Exception as e:
                                        err = f"[{can_id:04X}] {msg_info.name} | {data.hex()} | Decoding error: {e}"
                                        insert_row(ts, err.encode(), log_level_error_int, module_unknown_id_int)
                                        if can_id not in decode_failure_logged:
                                            decode_failure_logged.add(can_id)
                                            self.logger.error(
                                                "Decoding failed for CAN ID %04X (%s), not logging it again",
                                                can_id,
                                                msg_info.name,
                                                exc=e,
                                            )
                                        continue

                                    local_sig_map = msg_info.signal_map
                                    for k, v in decoded.items():
                                        insert_row(ts, str(v).encode(), log_level_info_int, local_sig_map[k])

                                else:
                                    # --- Unmapped ID Handling ---
                                    if strict:
                                        out_bytes = f"[{can_id:04X}] UNKNOWN CAN ID | {data.hex()}".encode()
                                        insert_row(ts, out_bytes, log_level_error_int, module_unknown_id_int)
                                    elif not ignore_unknown:
                                        out_bytes = f"[{can_id:04X}] UNMAPPED | {data.hex()}".encode()
                                        insert_row(ts, out_bytes, log_level_info_int, module_unknown_id_int)
                        finally:
                            timestamps.release()
                            offsets.release()
                            lengths.release()
                            buf.release()
                            can_ids.release()

                    # --- Check for Flush Thresholds: full, or its oldest row has waited `delay` ---
                    if batch_out and (
                        batch_out.size >= batch_out.capacity
                        or batch_out.msg_cursor >= (batch_out.buffer_capacity() * 0.9)
                        or (batch_out_time and time_ns() - batch_out_time >= max_timeout_ns)
                    ):
                        flush()
                except Exception as e:
                    # Catch batch-level errors, log them, and KEEP GOING
                    self.logger.exception("Batch processing error (thread continuing)", exc=e)
            # Final flush on exit
            flush()

        except Exception as e:
            self.logger.exception("run failure", exc=e)
