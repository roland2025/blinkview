# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from ..core.BaseBindableConfigurable import BaseBindableConfigurable
from ..core.factory import BaseFactory


class TransformStep(BaseBindableConfigurable):
    input_type: str = 'any'
    output_type: str = 'any'

    def __init(self):
        super().__init__()


class TransformerFactory(BaseFactory[TransformStep]):
    pass


class PipelinePrintableFactory(BaseFactory[TransformStep]):
    pass


class PipelineDecodeFactory(BaseFactory[TransformStep]):
    pass


class PipelineTransformFactory(BaseFactory[TransformStep]):
    pass

