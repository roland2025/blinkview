# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from blinkview.core.device_identity import ModuleIdentity
from blinkview.utils.log_level import LevelIdentity

_FLOAT_RE = re.compile(r"[-+]?\d*\.\d+(?:[eE][-+]?\d+)?|[-+]?\d+")
_FLOAT_RE_FINDALL = _FLOAT_RE.findall  # Cache the method for performance


@dataclass(slots=True)
class LogRow:
    timestamp_ns: int  # Store as absolute Unix nanoseconds
    level: LevelIdentity
    module: ModuleIdentity
    message: str
    seq: int = 0
    _values: Optional[tuple[float, ...]] = None

    def __lt__(self, other):
        return self.timestamp_ns < other.timestamp_ns

    @property
    def timestamp(self) -> float:
        """Helper to provide float seconds for legacy UI components if needed."""
        return self.timestamp_ns / 1_000_000_000.0

    def get_values(self) -> tuple[float, ...]:
        """
        Extracts all floating point numbers from the message.
        Caches the result in _values for subsequent calls.
        """
        if self._values is not None:
            return self._values

        # Find all matches and convert to float
        # Using a list comprehension here is faster than a loop
        self._values = tuple(float(val) for val in _FLOAT_RE_FINDALL(self.message))
        return self._values
