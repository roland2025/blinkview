# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

from ..core.numba_config import app_njit
from ..core.types.parsing import UnifiedParserConfig
from .strings import skip_n_words


class SkipWordsConfig(NamedTuple):
    count: int


@app_njit(inline="always")
def skip_words_parser(buffer, start_cursor, end_cursor, out_b, out_idx, state, config: UnifiedParserConfig):
    """
    Universal Parser Stage to skip a predefined number of words.
    Uses 'config.count' for the number of words.
    """
    # Simply call the tool and return the new cursor
    return skip_n_words(buffer, start_cursor, end_cursor, config.module_config.max_length)
