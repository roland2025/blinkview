# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.factory import BaseFactory
from blinkview.core.types.parsing import CodecID
from blinkview.ops.codecs import nb_parser_noop
from blinkview.parsers.frame_decoders import (
    FrameDecoder,
    FrameDecoderBase,
    FrameDecoderFactory,
    LineDecoder,
    PreFramedDecoder,
)


def configure(decoder, **overrides):
    """FrameDecoder.__init__ doesn't call super().__init__(), so schema defaults never get
    hydrated via plain construction - the real construction path (matching BaseFactory.build())
    is hydrate_config() then apply_config()."""
    hydrated = decoder.hydrate_config(overrides)
    decoder.apply_config(hydrated)
    return decoder


class TestFrameDecoderDefaults:
    def test_decode_starts_as_the_noop_parser(self):
        decoder = LineDecoder()
        assert decoder.decode is nb_parser_noop

    def test_default_bundle_matches_schema_defaults(self):
        decoder = configure(LineDecoder())
        bundle = decoder.bundle()

        assert bundle.delimiter == 10
        assert bundle.length_fixed is False  # frame_length_dynamic default True
        assert bundle.length_min == 1
        assert bundle.length_max == 1024
        assert bundle.length == 0
        assert bundle.filter_printable is False
        assert bundle.filter_ansi is False
        assert bundle.filter_trim_r is True
        assert bundle.report_error is True  # frame_errors_hidden default False

    def test_fixed_length_mode_uses_frame_length_doubled_as_the_max(self):
        decoder = configure(LineDecoder(), frame_length_dynamic=False, frame_length=64)
        bundle = decoder.bundle()

        assert bundle.length_fixed is True
        assert bundle.length_max == 128
        assert bundle.length == 64

    def test_frame_errors_hidden_flips_report_error_off(self):
        decoder = configure(LineDecoder(), frame_errors_hidden=True)
        assert decoder.bundle().report_error is False

    def test_filters_are_reflected_in_the_bundle(self):
        decoder = configure(LineDecoder(), filter_ansi=True, filter_printable=True, filter_trim_r=False)
        bundle = decoder.bundle()

        assert bundle.filter_ansi is True
        assert bundle.filter_printable is True
        assert bundle.filter_trim_r is False

    def test_bundle_is_none_before_apply_config(self):
        decoder = LineDecoder()
        assert decoder.bundle() is None


class TestDecoderSubclassCodecIds:
    def test_pre_framed_decoder_uses_codec_none(self):
        decoder = configure(PreFramedDecoder())
        assert decoder.bundle().decode_id == CodecID.NONE

    def test_line_decoder_uses_codec_newline(self):
        decoder = configure(LineDecoder())
        assert decoder.bundle().decode_id == CodecID.NEWLINE


class TestFactories:
    def test_frame_decoder_factory_is_a_base_factory_for_frame_decoder_base(self):
        assert issubclass(FrameDecoderFactory, BaseFactory)
        assert FrameDecoderFactory.produces_type is FrameDecoderBase

    def test_registered_types_are_reachable_via_the_factory(self):
        available = dict(FrameDecoderFactory.get_available_types())
        assert "none" in available
        assert "line_decoder" in available

    def test_frame_decoder_is_a_frame_decoder_base_subclass(self):
        assert issubclass(FrameDecoder, FrameDecoderBase)
