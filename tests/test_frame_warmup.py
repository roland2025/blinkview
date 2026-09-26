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
    FRAME_DECODER_WARMUPS,
    FRAME_SECTION_WARMUPS,
    frame_decoder_warmup,
    frame_section_warmup,
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


@pytest.mark.parametrize("monolith", [False, True])
def test_binary_parser_warmup_runs_every_registered_decoder_and_section(monkeypatch, monolith):
    monkeypatch.setattr(BinaryParser, "monolith", monolith)

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
