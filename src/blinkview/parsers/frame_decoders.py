# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable, configuration_property
from blinkview.core.factory import BaseFactory
from blinkview.core.types.frames import FrameConfig


@configurable
@bindable
class FrameDecoderBase:
    pass


class FrameDecoderFactory(BaseFactory[FrameDecoderBase]):
    pass


@configuration_property(
    "frame_delimiter",
    type="integer",
    title="Frame delimiter (byte value)",
    ui_order=10,
    minimum=0,
    maximum=255,
    default=10,
    description="Splits the input byte stream into frames based on a specified delimiter character. For example, setting the delimiter to ASCII 10 (newline) will split the stream into lines. This is useful for log formats where entries are separated by specific characters. If not set, the parser will treat the entire input as a single frame.",
)
@configuration_property(
    "filter_ansi",
    type="boolean",
    title="Filter ansi characters",
    ui_order=21,
    default=False,
    required=True,
    description="Filters ANSI escape sequences from the input byte stream. This is useful for cleaning up logs that contain color codes or other terminal control sequences, leaving only the raw text content for further processing.",
)
@configuration_property(
    "filter_printable",
    title="Filter non-printable characters",
    type="boolean",
    ui_order=20,
    default=False,
    required=True,
    description="Filters non-printable characters from the input byte stream. This can help clean up logs that contain binary data or control characters, ensuring that only human-readable text is processed in subsequent stages.",
)
@configuration_property(
    "frame_length_dynamic",
    type="boolean",
    title="Dynamic frame length",
    required=True,
    default=True,
    ui_order=30,
)
@configuration_property(
    "frame_length",
    type="integer",
    title="Frame payload length (fixed)",
    required=True,
    default=0,
    ui_order=32,
)
@configuration_property(
    "frame_length_minimum",
    type="integer",
    title="Minimum frame length (bytes)",
    default=1,
    required=True,
    ui_order=34,
)
@configuration_property(
    "frame_length_maximum",
    type="integer",
    title="Maximum frame length (bytes)",
    default=1024,
    required=True,
    ui_order=36,
)
@configuration_property(
    "filter_trim_r",
    type="boolean",
    title="Trim trailing CR",
    default=True,
    required=True,
    ui_order=15,
    description="When enabled, this option trims trailing carriage return characters (ASCII 13) from the end of each frame after splitting. This is particularly useful for handling logs from Windows environments, where lines often end with a carriage return followed by a newline (\\r\\n). Enabling this option helps clean up log entries by removing these extraneous characters, ensuring that the resulting frames contain only the intended log content.",
)
@configuration_property("frame_errors_hidden", type="boolean", title="Hide frame errors", required=True, default=False)
class FrameDecoder(FrameDecoderBase):
    frame_delimiter: int
    filter_ansi: bool
    filter_printable: bool
    frame_length_dynamic: bool
    frame_length: int
    filter_trim_r: bool
    frame_length_maximum: int
    frame_length_minimum: int
    frame_errors_hidden: bool

    def __init__(self):
        self.decode = None

    def bundle(self):
        return FrameConfig(
            decode_func=self.decode,
            delimiter=self.frame_delimiter,
            length_fixed=not self.frame_length_dynamic,
            length_min=self.frame_length_minimum,
            length_max=self.frame_length_maximum if self.frame_length_dynamic else self.frame_length * 2,
            length=self.frame_length,
            filter_printable=self.filter_printable,
            filter_ansi=self.filter_ansi,
            filter_trim_r=self.filter_trim_r,
            report_error=not self.frame_errors_hidden,
        )


@FrameDecoderFactory.register("line_decoder")
class LineDecoder(FrameDecoder):
    """Frame processor with no special encoding"""

    def __init__(self):
        from blinkview.ops.codecs import decode_newline_frame

        self.decode = decode_newline_frame


@FrameDecoderFactory.register("cobs_decoder")
class CobsDecoder(FrameDecoder):
    """Frame processor for COBS-encoded frames"""

    def __init__(self):
        from blinkview.ops.codecs import decode_cobs_frame

        self.frame_delimiter = 0x00
        self.decode = decode_cobs_frame


@FrameDecoderFactory.register("decode_slip_frame")
class SlipDecoder(FrameDecoder):
    """Frame processor for SLIP-encoded frames"""

    def __init__(self):
        from blinkview.ops.codecs import decode_slip_frame

        self.frame_delimiter = 0xC0
        self.decode = decode_slip_frame
