# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from struct import pack

from blinkview.storage.file_logger import FileLogger


class RawLogger(FileLogger):
    def __init__(self, name, session, max_batch=None, flush_interval=None):
        super().__init__(name=name, session=session, max_batch=max_batch, flush_interval=flush_interval)

    def extension(self):
        return ".bin"  # Binary format for replay

    def process_batch(self, batch):
        """
        Batch contains tuples of (timestamp_ns, device_id, raw_bytes, decoded)
        Format per entry:
        - <Q: Unsigned 64-bit int (Timestamp)
        - <H: Unsigned 16-bit short (Length of data)
        - Data bytes
        """
        if not self.file_handle:
            return

        packed_data = bytearray()
        for ts_ns, _, data, _ in batch:
            length = len(data)
            # Pack timestamp and length, then append raw data
            packed_data.extend(pack("<QH", ts_ns, length))
            packed_data.extend(data)

        self.file_handle.write(packed_data)
