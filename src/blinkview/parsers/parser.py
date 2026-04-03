# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter, sleep, time
from typing import Any, Callable, List

from ..core.base_daemon import BaseDaemon
from ..core.batch_queue import BatchQueue
from ..core.configurable import (
    configuration_factory,
    configuration_property,
    on_config_change,
    override_property,
)
from ..core.constants import SysCat
from ..core.device_identity import DeviceIdentity
from ..core.factory import BaseFactory
from ..core.limits import BATCH_MAXLEN
from ..core.log_row import LogRow
from ..core.reusable_batch_pool import TimeDataEntry
from ..utils.level_map import LogLevel

# Define the signature for a transformation
TransformFunc = Callable[[Any], Any]


@configuration_factory("parser")
@configuration_property(
    "max_batch",
    type="integer",
    default=BATCH_MAXLEN,
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

        pool_acquire = self.shared.pool.get(tag="LogRows").acquire

        parsed_batch = pool_acquire()
        batch_append = parsed_batch.append
        _perf_counter = perf_counter
        last_flush_time = _perf_counter()

        gui_mode = self.gui_mode
        _msg_since_yield = 0

        LogRowCtr = LogRow

        error = self.logger.error

        _split_char = self._split_char
        _print = self._print
        _decode = self._decode
        _transform = self._transform
        _assemble = self._assemble
        _len = len
        _str = str

        if _assemble is None:
            module_log = device_identity.get_module("log")

        module_unknown = None

        def flush():
            nonlocal parsed_batch, last_flush_time, batch_append
            if parsed_batch.size:
                # print(f"[{self.__class__.__name__}] {time()} Flushing: {parsed_batch}")
                with parsed_batch:
                    self.distribute(parsed_batch)

                parsed_batch = pool_acquire()
                batch_append = parsed_batch.append
                last_flush_time = _perf_counter()

        # Pre-allocate 1MB (or whatever fits your max expected "burst")
        BUFFER_CAPACITY = 64 * 1024
        buffer = bytearray(BUFFER_CAPACITY)
        buffer_find = buffer.find
        write_offset = 0  # Where we append new data
        read_offset = 0  # Where we start searching for split_char

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            time_remaining = max(0, (last_flush_time + max_timeout) - _perf_counter())
            batch = get(timeout=time_remaining)
            if not batch:
                # No data
                if parsed_batch:
                    flush()
                last_flush_time = _perf_counter()
                continue
            with batch:
                # print(f"[ParserThread] Receive {time()} batch={batch}")
                for entry in batch:
                    entry: TimeDataEntry
                    # print(f"[{self.__class__.__name__}] Got entry: {entry}")
                    # timestamp_ns, raw_data = entry
                    # self.logger.info(f"Timestamp: {timestamp_ns}, Raw data: {raw_data}")
                    # raw data is byte stream
                    # print(f"split: {self._split_char}")

                    if gui_mode:
                        _msg_since_yield += batch.size
                        if _msg_since_yield >= 25_000:
                            sleep(0.002)
                            _msg_since_yield = 0

                    if _split_char is not None:
                        try:
                            data = entry.data
                            data_len = _len(data)
                            if write_offset + data_len > BUFFER_CAPACITY:
                                # How much unparsed data is left?
                                unparsed_len = write_offset - read_offset

                                # Shift the "tail" to the front of the buffer
                                buffer[0:unparsed_len] = buffer[read_offset:write_offset]

                                # Reset pointers
                                read_offset = 0
                                write_offset = unparsed_len

                                # If it's STILL too small, we must grow (rare)
                                if write_offset + data_len > BUFFER_CAPACITY:
                                    buffer.extend(bytearray(data_len + BUFFER_CAPACITY))
                                    BUFFER_CAPACITY = _len(buffer)

                            buffer[write_offset : write_offset + data_len] = data
                            write_offset += data_len

                            while True:
                                split_index = buffer_find(_split_char, read_offset, write_offset)

                                # print(f"[{self.__class__.__name__}] Got entry: {split_index}")

                                if split_index == -1:
                                    break

                                if read_offset != split_index:
                                    prev_read_offset = read_offset
                                    read_offset = split_index + 1
                                    try:
                                        line = buffer[prev_read_offset:split_index]
                                        if _print is not None:
                                            line = _print(line)
                                            if not line:
                                                continue
                                            # self.logger.trace(f"printable: {ts}: '{line}'")

                                        if _decode is not None:
                                            line = _decode(line)
                                            if not line:
                                                continue
                                            # self.logger.trace(f"decoded: {ts}: '{line}'")

                                        if _transform is not None:
                                            line = _transform(line)
                                            if not line:
                                                continue
                                            # self.logger.trace(f"transformed: {ts}: '{line}'")

                                        if _assemble is not None:
                                            line = _assemble(entry.time, device_identity, line)
                                            # self.logger.trace(f"assembled: {ts}: '{line}'")
                                        else:
                                            line = LogRowCtr(entry.time, LogLevel.INFO, module_log, _str(line))

                                        batch_append(line)
                                    except Exception as e:
                                        if log_error_row_on_invalid:
                                            # print(f"[ParserThread] {device_identity.name} ... Error processing line. Error: {e}")
                                            if module_unknown is None:
                                                module_unknown = device_identity.get_module("_unknown")
                                            batch_append(
                                                LogRowCtr(
                                                    entry.time,
                                                    LogLevel.ERROR,
                                                    module_unknown,
                                                    _str(line),
                                                )
                                            )

                                if parsed_batch.size >= max_batch or (_perf_counter() - last_flush_time >= max_timeout):
                                    flush()

                        except Exception as e:
                            error(f"Error during splitting.", e)

                            if module_unknown is None:
                                module_unknown = device_identity.get_module("_unknown")
                            batch_append(
                                LogRowCtr(
                                    entry.time,
                                    LogLevel.ERROR,
                                    module_unknown,
                                    _str(entry.data),
                                )
                            )
                    else:
                        ts = entry.time
                        line = entry.data

                        try:
                            if _print is not None:
                                line = _print(line)
                                if not line:
                                    continue
                                # self.logger.trace(f"printable: {ts}: '{line}'")

                            if _decode is not None:
                                line = _decode(line)
                                if not line:
                                    continue
                                # self.logger.trace(f"decoded: {ts}: '{line}'")

                            if _transform is not None:
                                line = _transform(line)
                                if not line:
                                    continue
                                # self.logger.trace(f"transformed: {ts}: '{line}'")

                            if _assemble is not None:
                                line = _assemble(ts, device_identity, line)
                                # self.logger.trace(f"assembled: {ts}: '{line}'")
                            else:
                                line = LogRowCtr(ts, LogLevel.INFO, module_log, _str(line))

                            batch_append(line)
                        except Exception as e:
                            if log_error_row_on_invalid:
                                # print(f"[ParserThread] {device_identity.name} ... Error processing line. Error: {e}")
                                if module_unknown is None:
                                    module_unknown = device_identity.get_module("_unknown")
                                batch_append(
                                    LogRowCtr(
                                        ts,
                                        LogLevel.ERROR,
                                        module_unknown,
                                        _str(line),
                                    )
                                )

                    if parsed_batch.size >= max_batch or (_perf_counter() - last_flush_time >= max_timeout):
                        flush()

        # Flush any remaining batch on exit
        flush()
        parsed_batch.release()


@ParserFactory.register("serial_default")
@override_property("split", default={})  # Default to newline character for splitting
@override_property("printable", default={})
@override_property("decode", default={})
@override_property("transform", default={"type": "default", "steps": [{"type": "ansi_filter"}]})
@override_property("assembler", default={"type": "default", "message_index": 0})
class SerialParserThread(ParserThread):
    __doc__ = "Splitting enabled by default for serial logs, with the split character set to newline (ASCII 10). This is a common configuration for serial log streams, where each log entry is typically separated by a newline character. Users can still customize the split character or disable splitting entirely if their log format differs."
