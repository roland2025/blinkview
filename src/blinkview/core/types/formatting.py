# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple


class FormattingConfig(NamedTuple):
    show_ts: bool
    show_dev: bool
    show_lvl: bool
    show_mod: bool
