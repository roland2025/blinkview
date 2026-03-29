# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from ..core.bindable import bindable
from ..core.configurable import configurable
from ..core.factory import BaseFactory


@configurable
@bindable
class TransformStep:
    input_type: str = "any"
    output_type: str = "any"


class TransformerFactory(BaseFactory[TransformStep]):
    pass


class PipelinePrintableFactory(BaseFactory[TransformStep]):
    pass


class PipelineDecodeFactory(BaseFactory[TransformStep]):
    pass


class PipelineTransformFactory(BaseFactory[TransformStep]):
    pass
