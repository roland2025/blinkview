# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import sleep

from ..core.configurable import configuration_property
from ..utils.paths import resolve_config_path
from .BaseReader import BaseReader, DeviceFactory


@DeviceFactory.register("binary_file")
@configuration_property(
    "file_path",
    type="string",
    required=True,
    ui_type="file",
    ui_file_filter="Binary Files (*.bin *.dat *.raw);;All Files (*)",
    description="Path to the binary file to stream. Supports relative paths via resolve_config_path.",
)
@configuration_property(
    "chunk_size", type="integer", default=8, description="Number of bytes to read per injection 'tick'."
)
@configuration_property(
    "frequency", type="integer", default=100, description="Injection rate in Hz (times per second)."
)
@configuration_property(
    "loop", type="boolean", default=True, description="Restart from the beginning of the file when EOF is reached."
)
class BinaryFileReader(BaseReader):
    __doc__ = """A development replay tool for streaming raw binary data.

* Mimics a live data source by injecting file content at a fixed frequency.
* Generates 'Now' timestamps for un-timestamped raw data.
* Uses pathlib for robust cross-platform path handling.
"""

    file_path: str
    chunk_size: int
    frequency: int
    loop: bool

    def __init__(self):
        super().__init__()

    def run(self):
        # Setup and Path Resolution
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Resolve path and convert to a Path object
        path = Path(resolve_config_path(self.file_path))

        interval_s = 1.0 / max(1, self.frequency)
        chunk_size = self.chunk_size

        logger.info(f"Starting Binary Reader: {path} (@{self.frequency}Hz)")

        if not path.exists():
            logger.error(f"Binary file not found: {path}")
            return

        # Main Ingestion Loop
        f = None
        try:
            # open() accepts Path objects directly
            f = path.open("rb")
            _read = f.read
            _seek = f.seek

            while not stop_is_set():
                # Read the next raw chunk
                data = _read(chunk_size)

                # Handle End of File
                if not data:
                    if self.loop:
                        _seek(0)
                        logger.debug(f"Replay loop: Resetting {path.name}")
                        continue
                    else:
                        logger.info(f"Binary replay finished: {path.name}")
                        break

                # Create a synthetic 'arrival' timestamp for the pipeline
                # This mimics live hardware latency
                now = time_ns()

                # Distribute as (timestamp, payload) list
                self.distribute([(now, data)])

                # Precise-ish frequency control
                sleep(interval_s)

        except Exception as e:
            logger.error(f"Error in BinaryFileReader for {path.name}", e)
        finally:
            if f:
                f.close()
                logger.info(f"Binary file closed: {path.name}")
