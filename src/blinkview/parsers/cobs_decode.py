# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from .transformer import PipelineDecodeFactory, TransformStep
# from .BaseTransformStep import TransformStep, PipelineDecodeFactory
from ..core.configurable import configuration_property


@PipelineDecodeFactory.register("cobs_decode")
@configuration_property("on_error", type="string", enum=["ignore", "raise"], default="ignore",
                        description="How to handle malformed COBS frames (e.g., from connecting mid-stream).")
class CobsDecodeStep(TransformStep):
    __doc__ = "A pure byte-to-byte decoder step that removes Consistent Overhead Byte Stuffing (COBS) from a single frame."

    input_type = 'bytes'
    output_type = 'bytes'

    on_error: str

    def __init__(self):
        super().__init__()

        self.process = None

        from cobs.cobs import decode, DecodeError

        _raise_on_error = self.on_error == "raise"

        def fast_process(data: bytes) -> bytes:
            # Handle empty frames (e.g., back-to-back 0x00 bytes in the raw stream)
            if not data:
                return b''

            try:
                return decode(data)

            except DecodeError as e:
                # A DecodeError is highly likely on the very first frame when connecting
                # to an active socket:// stream. We safely absorb and drop it by default.
                if _raise_on_error:
                    raise ValueError(f"Corrupted COBS frame: {e}")

                # self.logger.debug(f"Dropped malformed COBS frame: {e}")
                return b''

        self.process = fast_process
