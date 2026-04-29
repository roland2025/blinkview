# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple


class FormattingConfig(NamedTuple):
    show_ts: bool = True
    show_dev: bool = True
    show_lvl: bool = True
    show_mod: bool = True
    ts_precision: int = 3
