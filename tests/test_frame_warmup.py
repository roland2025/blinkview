# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.frame_warmup_registry import (
    DEFAULT_FRAME_WARMUP_WEIGHT,
    FRAME_DECODER_WARMUPS,
    FRAME_SECTION_WARMUPS,
    FRAME_WARMUP_WEIGHTS,
    KIND_DECODER,
    KIND_SECTION,
    frame_decoder_warmup,
    frame_section_warmup,
    frame_warmup_weight,
)
from blinkview.core.warmup import NumbaWarmupHelper
from blinkview.parsers.binary_parser import BinaryParser
from blinkview.parsers.frame_decoders import FrameDecoderFactory
from blinkview.parsers.frame_parsers import FrameParserFactory, FrameSectionParserFactory


def test_decorators_register_the_config_and_return_the_class_unchanged():
    before_decoders, before_sections = list(FRAME_DECODER_WARMUPS), list(FRAME_SECTION_WARMUPS)
    try:

        class Dummy:
            pass

        assert frame_decoder_warmup("dec", frame_length=3)(Dummy) is Dummy
        assert frame_section_warmup("sec", count=2)(Dummy) is Dummy
        assert FRAME_DECODER_WARMUPS[-1] == {"type": "dec", "frame_length": 3}
        assert FRAME_SECTION_WARMUPS[-1] == {"type": "sec", "count": 2}
    finally:
        FRAME_DECODER_WARMUPS[:] = before_decoders
        FRAME_SECTION_WARMUPS[:] = before_sections
        FRAME_WARMUP_WEIGHTS.pop((KIND_DECODER, "dec"), None)
        FRAME_WARMUP_WEIGHTS.pop((KIND_SECTION, "sec"), None)


def test_warmup_weight_is_kept_out_of_the_config():
    before_decoders = list(FRAME_DECODER_WARMUPS)
    try:

        class Dummy:
            pass

        frame_decoder_warmup("heavy_dec", warmup_weight=8.0, frame_length=3)(Dummy)
        assert FRAME_DECODER_WARMUPS[-1] == {"type": "heavy_dec", "frame_length": 3}
        assert frame_warmup_weight(KIND_DECODER, "heavy_dec") == 8.0
        assert frame_warmup_weight(KIND_SECTION, "heavy_dec") == DEFAULT_FRAME_WARMUP_WEIGHT  # other kind
    finally:
        FRAME_DECODER_WARMUPS[:] = before_decoders
        FRAME_WARMUP_WEIGHTS.pop((KIND_DECODER, "heavy_dec"), None)


def test_binary_parser_warmup_reports_weighted_substeps(monkeypatch):
    monkeypatch.setattr(BinaryParser, "_warmup_config", staticmethod(lambda helper, decoder, parser: None))
    reported = []
    helper = SimpleNamespace(report_substep=lambda done, total: reported.append(done / total))

    BinaryParser.warmup(helper)

    weights = [frame_warmup_weight(KIND_DECODER, e["type"]) for e in FRAME_DECODER_WARMUPS]
    weights += [frame_warmup_weight(KIND_SECTION, e["type"]) for e in FRAME_SECTION_WARMUPS]
    assert len(reported) == len(weights)
    assert reported[0] == 0.0
    assert reported == sorted(reported)
    # Each step advances by its own weight's share, not by 1/len.
    steps = [b - a for a, b in zip(reported, reported[1:])]
    assert steps == pytest.approx([w / sum(weights) for w in weights[:-1]])


def test_every_registered_decoder_has_a_warmup_entry_and_vice_versa():
    warmed = [entry["type"] for entry in FRAME_DECODER_WARMUPS]
    assert sorted(set(warmed)) == sorted(FrameDecoderFactory._registry), "decoder without a warmup entry"
    assert len(warmed) == len(set(warmed))


def test_every_registered_section_has_a_warmup_entry_and_vice_versa():
    warmed = [entry["type"] for entry in FRAME_SECTION_WARMUPS]
    assert sorted(set(warmed)) == sorted(FrameSectionParserFactory._registry), "section without a warmup entry"
    assert len(warmed) == len(set(warmed))


def _helper():
    registry = FactoryRegistry()
    registry.register("frame_decoder", FrameDecoderFactory)
    registry.register("frame_parser", FrameParserFactory)
    registry.register("frame_section_parser", FrameSectionParserFactory)
    shared = SimpleNamespace(
        array_pool=NumpyArrayPool(), time_ns=time.time_ns, factories=registry, tasks=None, settings=None
    )
    return NumbaWarmupHelper(shared)


def test_binary_parser_warmup_runs_every_registered_decoder_and_section(monkeypatch):
    built = []
    original = BinaryParser._warmup_config

    def spy(helper, decoder_config, parser_config):
        built.append((decoder_config["type"], [step["type"] for step in parser_config["steps"]]))
        return original(helper, decoder_config, parser_config)

    monkeypatch.setattr(BinaryParser, "_warmup_config", staticmethod(spy))

    helper = _helper()
    try:
        BinaryParser.warmup(helper)
    finally:
        helper.log_pool.release_all()

    assert [decoder for decoder, steps in built if not steps] == [e["type"] for e in FRAME_DECODER_WARMUPS]
    assert [steps[0] for _decoder, steps in built if steps] == [e["type"] for e in FRAME_SECTION_WARMUPS]
