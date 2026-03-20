# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.BaseBindableConfigurable import BaseBindableConfigurable
from blinkview.core.factory import BaseFactory

from abc import ABC, abstractmethod
from typing import Any


class BaseAssembler(BaseBindableConfigurable):
    pass


class AssemblerFactory(BaseFactory[BaseAssembler]):
    pass
