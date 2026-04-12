# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any, Callable, NamedTuple, Tuple

import numpy as np


class ParserConfig(NamedTuple):
    level_default: int
    level_error: int
    module_unknown: int
    module_log: int
    device_id: int
    report_error: bool
    filter_squash_spaces: bool


class InputParams(NamedTuple):
    ts_in: int  # Timestamp for this batch
    split_char: int  # Delimiter (e.g., ord('\n'))
    def_lvl: int  # Default Level
    def_mod: int  # Default Module ID
    def_dev: int  # Default Device ID


class LogOutput(NamedTuple):
    ts: np.ndarray  # int64
    off: np.ndarray  # uint32
    lengths: np.ndarray  # uint32
    buf: np.ndarray  # uint8
    # Optional columns (Pass empty arrays if not used)
    lvl: np.ndarray  # uint8
    mod: np.ndarray  # uint16
    dev: np.ndarray  # uint16
    seq: np.ndarray  # uint64
    # Status flags
    has_lvl: bool
    has_mod: bool
    has_dev: bool
    has_seq: bool


class ParserPipelineBundle(NamedTuple):
    """
    The complete, bundled state required for the Parser logic in the Numba kernel.
    """

    config: ParserConfig
    pipeline: Tuple[Tuple[Callable, Any, Any], ...]
