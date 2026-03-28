# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from can import Message

from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.log_row import LogRow
from blinkview.parsers.assembler import BaseAssembler
from blinkview.parsers.can_bus import CanAssemblerFactory, CanParserFactory
from blinkview.utils.level_map import LogLevel

from ..core.base_configurable import configuration_property
from ..io.BaseReader import DeviceFactory
from .cantools_decoder import can_msg_to_str
from .parser import BaseParser, ParserFactory


@CanAssemblerFactory.register("cantools")
@configuration_property(
    "prepend_msg_name",
    type="boolean",
    default=False,
    description="If enabled, groups signals under their DBC message name (e.g., 'BMS_Status_1.voltage'). If disabled, signals are flat (e.g., 'voltage').",
)
class CantoolsToLogRow(BaseAssembler):
    __doc__ = "Converts msgpack-encoded log lines into LogRow objects. Expects the msgpack format to be: (created, levelno, name, msg)."

    def __init__(self):
        super().__init__()

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        self._bake()
        return changed

    def _bake(self):
        # Cache frequently used objects locally
        INFO = LogLevel.INFO
        LogRowCtor = LogRow
        prepend = self.prepend_msg_name

        def fast_parse(created: int, dev_id: DeviceIdentity, line: tuple):
            can_id, msg_name, signals = line  # Unpack the tuple returned by the decoder
            res = []

            for signal_name, signal_value in signals.items():
                # For each signal, create a LogRow. The module can be determined by the message name or CAN ID.
                if prepend:
                    signal_name = f"{msg_name}.{signal_name}"
                module = dev_id.get_module(signal_name)
                if isinstance(signal_value, float):
                    signal_value = f"{signal_value:.3f}"
                else:
                    signal_value = str(signal_value)
                res.append(LogRowCtor(created, INFO, module, signal_value))

            return res

        self.process = fast_parse

    def process(self, timestamp, dev_id, line):
        raise RuntimeError("ID Registry must be set before parsing.")


from time import perf_counter
from typing import Any

from ..core.base_configurable import (
    configuration_property,
    on_config_change,
    override_property,
)
from ..core.log_row import LogRow
from ..utils.level_map import LogLevel


@configuration_property(
    "decode",
    type="object",
    ui_order=10,
    _factory="can_decode",
    _factory_default="cantools",
    description="Decodes raw can.Message objects into structured data (e.g., via Cantools/DBC or struct unpack).",
)
@configuration_property(
    "transform",
    type="object",
    ui_order=11,
    _factory="can_transform",
    _factory_default="default",
    description="Data transformation steps to apply to the decoded CAN payload.",
)
@configuration_property(
    "assembler",
    title="Message parser",
    type="object",
    ui_order=12,
    _factory="can_assembler",
    _factory_default="cantools",
    description="Assembles transformed CAN data into final LogRow objects, automatically routing to the correct module based on CAN ID.",
)
@configuration_property("sources_", type="string", required=True, _reference="/sources", default="")
@ParserFactory.register("can")
class CANparser(BaseParser):
    __doc__ = """The specialized pipeline for processing discrete CAN bus frames.

* Directly accepts python-can Message objects.
* Bypasses byte-stream splitting for maximum performance.
* Routes payloads through decoding (e.g., DBC file parsing) and transformations.
* Assembles the final data into LogRow objects mapped to specific CAN IDs.

Because CAN frames are already discrete packets, this parser avoids the overhead of buffer management and string encoding, making it ideal for high-frequency vehicle telemetry."""

    def __init__(self):
        super().__init__()

        self._decoder = None
        self._decode = None

        self._transformer = None
        self._transform = None

        self._assembler = None
        self._assemble = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        self.logger.info(f"Applying CAN parser config: {config}")
        factory_build = self.shared.factories.build

        decoder_cfg = config.get("decode")
        if decoder_cfg:
            self._decoder = factory_build("can_decode", decoder_cfg, self.shared)
            self._decode = self._decoder.process
        else:
            self._decode = None
            self._decoder = None

        transformer_cfg = config.get("transform", {})
        if transformer_cfg:
            self._transformer = factory_build("can_transform", transformer_cfg, system_ctx=self.shared)
            self._transform = self._transformer.process
        else:
            self._transform = None
            self._transformer = None

        assembler_cfg = config.get("assembler", {})
        if assembler_cfg:
            self._assembler = factory_build("can_assembler", assembler_cfg, system_ctx=self.shared)
            self._assemble = self._assembler.process
        else:
            self._assembler = None
            self._assemble = None

        self.thread_needs_restart = True
        return changed

    @on_config_change("name")
    def name_changed(self, name, old):
        self.logger.info(f"Device name changed from '{old}' to '{name}'")
        dev_id: DeviceIdentity = self.local.device_id
        dev_id.name = name

    def run(self):
        self.logger.info("Starting CAN parser thread")

        # Localize lookups for the hot loop
        get = self.input_queue.get
        max_batch = self.max_batch
        max_timeout = self.delay / 1000.0
        stop_is_set = self._stop_event.is_set
        device_identity = self.local.device_id

        parsed_batch = []
        last_flush_time = perf_counter()

        # Fallback modules if no assembler is defined
        module_can = device_identity.get_module("bus")
        module_unknown = None

        can_msg_to_str_ = can_msg_to_str

        def flush():
            nonlocal parsed_batch, last_flush_time
            if parsed_batch:
                self.distribute(parsed_batch)
                parsed_batch = []
                last_flush_time = perf_counter()

        while not stop_is_set():
            # if the queue is empty and max_timeout was exceeded during processing.
            time_remaining = max(0.01, (last_flush_time + max_timeout) - perf_counter())

            try:
                batch = get(timeout=time_remaining)
                # print(f"Got batch of {len(batch)} CAN messages from queue.")
            except Exception:  # Queue Empty
                batch = None

            if not batch:
                if parsed_batch:
                    flush()
                last_flush_time = perf_counter()
                continue

            for timestamp_ns, can_msg in batch:
                can_msg_original: "Message" = can_msg  # Keep the original message for error reporting
                # print(f"Received CAN message: {can_msg} at {timestamp_ns}")

                try:
                    # Decode (e.g., apply cantools DBC mapping to dict)
                    if self._decode is not None:
                        can_msg = self._decode(can_msg)
                        # self.logger.trace(f"decoded: {timestamp_ns}: {can_msg}")

                    # Transform (e.g., math operations on specific fields)
                    if self._transform is not None:
                        can_msg = self._transform(timestamp_ns, can_msg_original.arbitration_id, can_msg)

                    # Assemble (Map to LogRow and specific ModuleIdentity)
                    if self._assemble is not None:
                        can_msg = self._assemble(timestamp_ns, device_identity, can_msg)
                        # self.logger.trace(f"assembled: {timestamp_ns}: {can_msg}")
                    else:
                        can_msg = LogRow(
                            timestamp_ns,
                            LogLevel.INFO,
                            module_can,
                            can_msg_to_str_(can_msg_original),
                        )

                    if isinstance(can_msg, list):
                        parsed_batch.extend(can_msg)
                    else:
                        parsed_batch.append(can_msg)

                except Exception as e:
                    if module_unknown is None:
                        module_unknown = device_identity.get_module("_unknown")
                    parsed_batch.append(
                        LogRow(
                            timestamp_ns,
                            LogLevel.ERROR,
                            module_unknown,
                            can_msg_to_str_(can_msg_original),
                        )
                    )

            # Time or Size-based flush check
            if len(parsed_batch) >= max_batch or (perf_counter() - last_flush_time >= max_timeout):
                flush()

        # Flush any remaining batch on exit
        flush()
