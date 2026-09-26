# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import UnifiedParserConfig
from blinkview.ops.stage_loop import nb_stage_loop_a1
from blinkview.ops.strings import nb_skip_n_words


class SkipWordsConfig(NamedTuple):
    count: int


@app_njit(inline="always")
def nb_skip_words_parser_optimized(buffer, start_cursor, end_cursor, out_b, out_idx, count):
    """
    Universal Parser Stage to skip a predefined number of words.
    Uses 'config.count' for the number of words.
    """
    # Simply call the tool and return the new cursor
    return nb_skip_n_words(buffer, start_cursor, end_cursor, count)


@app_njit(inline="always")
def nb_skip_words_parser(buffer, start_cursor, end_cursor, out_b, out_idx, state, config: UnifiedParserConfig):
    """Unified-struct entry point; the logic lives in nb_skip_words_parser_optimized."""
    return nb_skip_words_parser_optimized(
        buffer, start_cursor, end_cursor, out_b, out_idx, config.module_config.max_length
    )


@app_njit()
def nb_skip_words_parser_stage(out_b0, f_state, n, count):
    nb_stage_loop_a1(nb_skip_words_parser_optimized, out_b0, f_state, n, count)
