# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter, sleep
from typing import Any, Callable, List

from ..core.configurable import (
    configuration_factory,
    configuration_property,
    on_config_change,
    override_property,
)
from ..core.base_daemon import BaseDaemon
from ..core.batch_queue import BatchQueue
from ..core.constants import SysCat
from ..core.device_identity import DeviceIdentity
from ..core.factory import BaseFactory
from ..core.log_row import LogRow
from ..utils.level_map import LogLevel

# Define the signature for a transformation
TransformFunc = Callable[[Any], Any]


@configuration_factory("parser")
@configuration_property(
    "max_batch",
    type="integer",
    default=200,
    description="Maximum number of log entries to buffer before flushing",
    ui_order=1,
)
@configuration_property(
    "delay",
    type="integer",
    default=30,
    description="Maximum time (in milliseconds) to hold a batch before flushing",
    ui_order=2,
)
@configuration_property(
    "sources_",
    type="array",
    required=True,
    items={"type": "string", "_reference": "/sources"},
    default=[],
)
@configuration_property(
    "name",
    type="string",
    default="pipeline",
    required=True,
    description="Name of the source device (for logging purposes)",
)
@configuration_property(
    "_note",
    title="Note",
    type="string",
    ui_order=-1,
    description="Add a not for your own reference.",
)
class BaseParser(BaseDaemon):
    max_batch: int
    delay: int
    name: str

    def __init__(self):
        super().__init__()
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put

        self.targets: List[SysCat] = [SysCat.REORDER, SysCat.STORAGE]


class ParserFactory(BaseFactory[BaseParser]):
    pass


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
    "decode",
    type="object",
    ui_order=30,
    _factory="pipeline_decode",
    _factory_default="bytes_decode",
    description="Converts raw bytes into a string format.",
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
class ParserThread(BaseParser):
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
            self._split_char = bytes([int(split_cfg.get("char", 10))])
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

        log_error_row_on_invalid = not self.ignore_invalid

        device_identity = self.local.device_id

        parsed_batch = []
        last_flush_time = perf_counter()

        if self._assemble is None:
            module_log = device_identity.get_module("log")

        module_unknown = None

        def flush():
            nonlocal parsed_batch, last_flush_time
            if parsed_batch:
                self.distribute(parsed_batch)
                parsed_batch = []
                last_flush_time = perf_counter()

        buffer = bytearray()

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            time_remaining = max(0, (last_flush_time + max_timeout) - perf_counter())
            batch = get(timeout=time_remaining)
            if not batch:
                # No data
                if parsed_batch:
                    flush()
                last_flush_time = perf_counter()
                continue

            for entry in batch:
                # self.logger.info(f"Got entry: {entry}")
                timestamp_ns, raw_data = entry
                # self.logger.info(f"Timestamp: {timestamp_ns}, Raw data: {raw_data}")
                # raw data is byte stream
                # print(f"split: {self._split_char}")

                lines = []

                if self._split_char is not None:
                    try:
                        buffer.extend(raw_data)
                        # self.logger.info(f"Buffer after extend: {buffer}")
                        while True:
                            split_index = buffer.find(self._split_char)
                            # self.logger.info(f"Split index: {split_index}")
                            if split_index == -1:
                                break
                            line_bytes = buffer[:split_index]
                            del buffer[: split_index + 1]
                            if line_bytes:
                                lines.append((timestamp_ns, line_bytes))
                            # self.logger.info(f"Split line: {line_bytes}")
                            # self.logger.info(f"Processing line: {line_bytes}")
                    except Exception as e:
                        self.logger.error(f"Error during splitting.", e)

                        if module_unknown is None:
                            module_unknown = device_identity.get_module("_unknown")
                        parsed_batch.append(
                            LogRow(
                                timestamp_ns,
                                LogLevel.ERROR,
                                module_unknown,
                                str(raw_data),
                            )
                        )
                else:
                    lines.append((timestamp_ns, raw_data))

                for ts, line in lines:
                    # line_original = line
                    # self.logger.trace(f"Processing {ts}: '{line}'")

                    try:
                        if self._print is not None:
                            line = self._print(line)
                            if not line:
                                continue
                            # self.logger.trace(f"printable: {ts}: '{line}'")

                        if self._decode is not None:
                            line = self._decode(line)
                            if not line:
                                continue
                            # self.logger.trace(f"decoded: {ts}: '{line}'")

                        if self._transform is not None:
                            line = self._transform(line)
                            if not line:
                                continue
                            # self.logger.trace(f"transformed: {ts}: '{line}'")

                        if self._assemble is not None:
                            line = self._assemble(ts, device_identity, line)
                            # self.logger.trace(f"assembled: {ts}: '{line}'")
                        else:
                            line = LogRow(timestamp_ns, LogLevel.INFO, module_log, str(line))

                        parsed_batch.append(line)
                    except Exception as e:
                        if log_error_row_on_invalid:
                            # print(f"[ParserThread] {device_identity.name} ... Error processing line. Error: {e}")
                            if module_unknown is None:
                                module_unknown = device_identity.get_module("_unknown")
                            parsed_batch.append(
                                LogRow(
                                    timestamp_ns,
                                    LogLevel.ERROR,
                                    module_unknown,
                                    str(line),
                                )
                            )

                    if len(parsed_batch) >= max_batch:
                        flush()

                if parsed_batch and (perf_counter() - last_flush_time >= max_timeout):
                    flush()

        # Flush any remaining batch on exit
        flush()


@ParserFactory.register("serial_default")
@override_property("split", default={})  # Default to newline character for splitting
@override_property("printable", default={})
@override_property("decode", default={})
@override_property("transform", default={"type": "default", "steps": [{"type": "ansi_filter"}]})
@override_property("assembler", default={"type": "default", "message_index": 0})
class SerialParserThread(ParserThread):
    __doc__ = "Splitting enabled by default for serial logs, with the split character set to newline (ASCII 10). This is a common configuration for serial log streams, where each log entry is typically separated by a newline character. Users can still customize the split character or disable splitting entirely if their log format differs."
