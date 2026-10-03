# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Dev mode: turns on BlinkView's own pipeline diagnostics (Speedometer/ThroughputAutoTuner
stats and tuner loggers) - see plans/dev-mode.md. Off by default: those lines describe
BlinkView, not the target, and would otherwise be most of every session."""

import os
from typing import Any, Optional

DEV_MODE_SETTING = "dev_mode"
DEV_MODE_ENV = "BLINKVIEW_DEV"

_TRUE_STRINGS = frozenset({"true", "1", "yes", "on"})


def parse_flag(value: Any) -> bool:
    """A setting's on/off value. `blink config set` stores strings ("true"), a hand-edited
    settings.json may hold a real bool; anything unrecognised counts as off."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in _TRUE_STRINGS
    return False


def resolve_dev_mode(settings) -> bool:
    """BLINKVIEW_DEV (when set) wins over the dev_mode setting."""
    env_value: Optional[str] = os.environ.get(DEV_MODE_ENV)
    if env_value is not None:
        return parse_flag(env_value)
    if settings is None:
        return False
    return parse_flag(settings.get(DEV_MODE_SETTING))
