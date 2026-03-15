# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.factory import BaseFactory
from blinkview.parsers.assembler import BaseAssembler
from blinkview.parsers.parser import BaseParser
from blinkview.parsers.transformer import TransformStep


class CanTransformFactory(BaseFactory[TransformStep]):
    pass


class CanParserFactory(BaseFactory[BaseParser]):
    pass


class CanDecoderFactory(BaseFactory[TransformStep]):
    pass


class CanAssemblerFactory(BaseFactory[BaseAssembler]):
    pass
