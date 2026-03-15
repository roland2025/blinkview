# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.utils.log_level import LogLevel


class ModuleGUIMeta:
    def __init__(self, current_filter_capacity=0):
        # We store the LogLevel for specific log_filter
        # Index 0 = Tab A, Index 1 = Tab B, etc.
        self.filter_conf = [LogLevel.ALL] * current_filter_capacity
