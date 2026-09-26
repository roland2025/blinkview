# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Registry of the frame decoders and frame parser sections that BinaryParser's warmup has to compile.

Every decoder and every pipeline section owns its own kernel (decoder.kernel / section.kernel), compiled the first
time it is called. Each concrete class registers a config that builds it; BinaryParser.warmup() then drives one
dummy batch through every registered decoder, and through every registered section behind a default decoder, so
all kernels are compiled in one go at startup instead of on the first real batch.

    @FrameSectionParserFactory.register("skip_words")
    @frame_section_warmup("skip_words", count=1)
    class SkipWordsParser(FrameSectionParser): ...

The type name must match the factory name; tests/test_frame_warmup.py checks that every registered factory type has
an entry and that every entry builds. Like warmup_registry, this module has no imports so the parser modules can
decorate their classes without cycles."""

from typing import Callable, Dict, List

FRAME_DECODER_WARMUPS: List[Dict] = []
FRAME_SECTION_WARMUPS: List[Dict] = []


def _register(target: List[Dict], type_name: str, config: Dict) -> Callable:
    entry = {"type": type_name, **config}

    def decorator(cls):
        target.append(entry)
        return cls

    return decorator


def frame_decoder_warmup(type_name: str, **config) -> Callable:
    """Class decorator: registers `{"type": type_name, **config}` as a frame_decoder config to warm up."""
    return _register(FRAME_DECODER_WARMUPS, type_name, config)


def frame_section_warmup(type_name: str, **config) -> Callable:
    """Class decorator: registers `{"type": type_name, **config}` as a frame parser step config to warm up."""
    return _register(FRAME_SECTION_WARMUPS, type_name, config)
