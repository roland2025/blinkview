# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter

from blinkview.core.base_configurable import override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.log_row import LogRow
from blinkview.parsers.parser import ParserFactory, BaseParser


@ParserFactory.register("key_value")
@override_property("sources_", items={"type": "string", "_reference": "/pipelines"},)
class KeyValueParser(BaseParser):

    def run(self):
        self.logger.info("Starting key-value extractor thread")
        get = self.input_queue.get
        max_batch = self.max_batch
        max_timeout = self.delay / 1000.0  # Convert milliseconds to seconds

        device_identity: DeviceIdentity = self.local.device_id
        system_identity = self.shared.id_registry.get_device("SYSTEM")

        parsed_batch = []
        last_flush_time = perf_counter()

        def flush():
            nonlocal parsed_batch, last_flush_time
            if parsed_batch:
                self.distribute(parsed_batch)
                parsed_batch = []
                last_flush_time = perf_counter()

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            now = perf_counter()
            time_remaining = max(0, (last_flush_time + max_timeout) - now)
            batch = get(timeout=time_remaining)
            if not batch:
                # No data
                if parsed_batch:
                    flush()
                last_flush_time = perf_counter()
                continue

            for entry in batch:
                entry: LogRow
                device_id = entry.module.device
                if device_id == device_identity or device_id == system_identity:
                    # this is us, skip it to avoid loops
                    continue
                # print(f"Got entry: {entry}")

                if "=" in entry.message:
                    # print(f"Parsing entry: {entry.message}")
                    splitted = entry.message.split(" ")
                    for key_value in splitted:
                        if "=" in key_value:
                            key, value = key_value.split("=", 1)
                            value = value.rstrip(",;")
                            if key and value:
                                try:
                                    module = device_identity.get_module(f"{entry.module.name}.{key}")
                                    parsed_batch.append(LogRow(entry.timestamp_ns, entry.level, module, value))
                                except Exception as e:
                                    # self.logger.error(f"Failed to create module for key '{key}': {e}")
                                    pass

                    if len(parsed_batch) >= max_batch:
                        flush()

            now = perf_counter()
            if parsed_batch and (now - last_flush_time >= max_timeout):
                flush()

        # Flush any remaining batch on exit
        flush()
