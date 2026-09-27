# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable, configuration_property
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.frame_warmup_registry import frame_decoder_warmup
from blinkview.core.types.frames import FrameConfig
from blinkview.core.types.parsing import CodecID


@configurable
@bindable
class FrameDecoderBase:
    pass


@register_factory_category(FactoryCategory.FRAME_DECODER)
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
@configuration_property(
    "frame_resync_on_start",
    type="boolean",
    title="Discard data before first delimiter",
    default=True,
    required=True,
    ui_order=38,
    description="When enabled, everything received before the first frame delimiter is discarded, because the stream may have been joined in the middle of a frame (e.g. UART). Disable for sources that always start on a frame boundary (e.g. TCP, files, ADB), so the first frame is not lost.",
)
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
    frame_resync_on_start: bool

    def __init__(self):
        from blinkview.ops.codecs import nb_decode_frames_passthrough, nb_parser_noop

        self.decode = nb_parser_noop
        self.codec_id = CodecID.NONE
        self._bundle = None
        # The decoder kernel: (frame config, f_state, input bundle, parser config, output config, output
        # bundle) -> (out_full, nframes). Subclasses with a real frame function replace it.
        self._kernel = nb_decode_frames_passthrough

    def apply_config(self, config: dict):

        changed = self.apply_base_config(config)
        self._bundle = FrameConfig(
            decode_id=self.codec_id,
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
        return changed

    def bundle(self):
        return self._bundle

    def kernel(self, f_state, in_b, p_config, o_config, out_b):
        """Splits the input batch into frames, decodes them and reserves one output row per frame.
        Returns (out_full, nframes); the parser sections then run over the nframes rows."""
        return self._kernel(self._bundle, f_state, in_b, p_config, o_config, out_b)


@FrameDecoderFactory.register("none")
@frame_decoder_warmup("none")
class PreFramedDecoder(FrameDecoder):
    """For pre-framed data"""

    def __init__(self):
        super().__init__()

        self.codec_id = CodecID.NONE


@FrameDecoderFactory.register("line_decoder")
@frame_decoder_warmup("line_decoder")
class LineDecoder(FrameDecoder):
    """Frame processor with no special encoding"""

    def __init__(self):
        super().__init__()
        from blinkview.ops.codecs import nb_decode_frames_newline

        self.codec_id = CodecID.NEWLINE
        self._kernel = nb_decode_frames_newline


@FrameDecoderFactory.register("cobs_decoder")
@frame_decoder_warmup("cobs_decoder")
class CobsDecoder(FrameDecoder):
    """Frame processor for COBS-encoded frames"""

    def __init__(self):
        super().__init__()
        self.frame_delimiter = 0x00

        self.codec_id = CodecID.COBS


@FrameDecoderFactory.register("decode_slip_frame")
@frame_decoder_warmup("decode_slip_frame")
class SlipDecoder(FrameDecoder):
    """Frame processor for SLIP-encoded frames"""

    def __init__(self):
        super().__init__()
        self.frame_delimiter = 0xC0
        self.codec_id = CodecID.SLIP
