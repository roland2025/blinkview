# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.base_configurable import BaseConfigurable
from blinkview.core.system_context import SystemContext
from blinkview.core.logger import SystemLogger


class BaseBindableConfigurable(BaseConfigurable):
    def __init__(self):
        super().__init__()

        self.shared: SystemContext = None
        self.local: SimpleNamespace = None

        self.logger: SystemLogger = None

    def bind_system(self, shared: SystemContext, local: SimpleNamespace):
        self.shared = shared
        self.local = local
        if hasattr(local, "get_logger"):
            self.logger = local.get_logger()
