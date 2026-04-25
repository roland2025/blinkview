# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from ..core.device_identity import DeviceIdentity
from ..core.numpy_batch_manager import PooledLogBatch
from ..utils.log_level import LogLevel
from ..utils.paths import resolve_config_path
from ..utils.throughput import Speedometer, ThroughputAutoTuner

if TYPE_CHECKING:
    from can import Message

from ..core.configurable import configuration_property
from .parser import BaseParser, ParserFactory


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
    description="If true, raises an error for unknown CAN IDs. Overrides ignore_unknown.",
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
                self.logger.info(f"Loading DBC file: {self.dbc_file}")
                from cantools import database

                self.db = database.load_file(resolve_config_path(self.dbc_file))
            except Exception as e:
                self.logger.error(f"Failed to load DBC file: {self.dbc_file}", e)
                self.db = None

        # --- Pre-bake Logic ---
        self._msg_info_map.clear()

        if self.db:
            get_device_module = self.local.device_id.get_module

            for msg in self.db.messages:
                # Pre-calculate the module IDs exclusively for this message's signals
                local_signal_map = {signal.name: get_device_module(signal.name).id for signal in msg.signals}

                self._msg_info_map[msg.frame_id] = DBCMsgInfo(
                    name=msg.name, decode=msg.decode, signal_map=local_signal_map
                )

                print(f"{msg.name}: {local_signal_map}")

        return changed

    def run(self):
        try:
            self.logger.info("Starting Cantools parser thread")
            get = self.input_queue.get

            # Using 50ms as a safe default delay if not specifically overridden
            max_timeout = getattr(self, "delay", 50) / 1000.0
            stop_is_set = self._stop_event.is_set

            device: DeviceIdentity = self.local.device_id

            device_id_int = device.id

            get_device_module = device.get_module

            module_unknown_id_int = get_device_module("unknown").id

            info_map_get = self._msg_info_map.get
            strict = getattr(self, "strict", False)
            ignore_unknown = getattr(self, "ignore_unknown", False)

            log_level_error_int = LogLevel.ERROR.value
            log_level_info_int = LogLevel.INFO.value

            pool_create = self.shared.array_pool.create

            # --- Auto-Tuning Trackers ---
            speed_in = Speedometer(logger=self.logger.child("stats_in"))
            speed_out = Speedometer(logger=self.logger.child("stats_out"))
            tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

            def batch_acquire():
                return pool_create(
                    PooledLogBatch,
                    tuner_out.estimated_capacity,
                    tuner_out.estimated_buffer_bytes,
                    has_levels=True,
                    has_modules=True,
                    has_devices=True,
                )

            batch_out = None

            def flush():
                nonlocal batch_out
                if batch_out is not None and batch_out.size > 0:
                    with batch_out:
                        tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=max_timeout)
                        self.distribute(batch_out)
                batch_out = None

            while not stop_is_set():
                batch_in = get(timeout=max_timeout)

                if not batch_in:
                    flush()
                    continue

                with batch_in:
                    speed_in.batch(batch_in)

                    # print(f"[cantools] batch_in={batch_in}")
                    # for ts, msg, _, _, _, _, addr, flags, _ in batch_in:
                    #     print(f"  ts={ts} addr={addr:04X} flags={flags:02X} data='{msg.tobytes().hex()}'")

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
                    ts = b_in.timestamps
                    offsets = b_in.offsets
                    lengths = b_in.lengths
                    buf = b_in.buffer

                    # Extension column coming from CANReader
                    can_ids = b_in.ext_u32_1

                    # 2. Extract, Decode, and Insert Loop
                    for i in range(size):
                        can_id = can_ids[i]
                        off = offsets[i]
                        data = buf[off : off + lengths[i]].tobytes()

                        msg_info = info_map_get(can_id)

                        if msg_info is not None:
                            try:
                                decoded = msg_info.decode(data)
                                local_sig_map = msg_info.signal_map

                                for k, v in decoded.items():
                                    mod_id = local_sig_map[k]
                                    out_bytes = str(v).encode()

                                    if not batch_out.insert(
                                        ts[i], out_bytes, log_level_info_int, mod_id, device_id_int
                                    ):
                                        flush()
                                        batch_out = batch_acquire()
                                        batch_out.insert(ts[i], out_bytes, log_level_info_int, mod_id, device_id_int)

                            except Exception as e:
                                out_bytes = (
                                    f"[{can_id:04X}] {msg_info.name} | {data.hex()} | Decoding error: {e}".encode()
                                )

                                if not batch_out.insert(
                                    ts[i], out_bytes, log_level_error_int, module_unknown_id_int, device_id_int
                                ):
                                    flush()
                                    batch_out = batch_acquire()
                                    batch_out.insert(
                                        ts[i], out_bytes, log_level_error_int, module_unknown_id_int, device_id_int
                                    )

                        else:
                            # --- Unmapped ID Handling ---
                            if strict:
                                raise ValueError(f"Unknown CAN ID: {can_id}")

                            if not ignore_unknown:
                                out_bytes = f"[{can_id:04X}] UNMAPPED | {data.hex()}".encode()
                                if not batch_out.insert(
                                    ts[i], out_bytes, log_level_info_int, module_unknown_id_int, device_id_int
                                ):
                                    flush()
                                    batch_out = batch_acquire()
                                    batch_out.insert(
                                        ts[i], out_bytes, log_level_info_int, module_unknown_id_int, device_id_int
                                    )

                # --- Check for Flush Thresholds ---
                if batch_out and (
                    batch_out.size >= batch_out.capacity or batch_out.msg_cursor >= (batch_out.buffer_capacity() * 0.9)
                ):
                    flush()

            # Final flush on exit
            flush()

        except Exception as e:
            self.logger.exception("run failure", e)
