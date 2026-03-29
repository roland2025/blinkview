# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from abc import ABC, abstractmethod
from typing import Any

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable
from blinkview.core.factory import BaseFactory


@configurable
@bindable
class BaseAssembler:
    pass


class AssemblerFactory(BaseFactory[BaseAssembler]):
    pass
