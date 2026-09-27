# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.stage_loop import nb_stage_loop_a1
from blinkview.ops.strings import nb_skip_n_words


@app_njit(inline="always")
def nb_skip_words_parser(buffer, start_cursor, end_cursor, out_b, out_idx, count):
    """
    Universal Parser Stage to skip a predefined number of words.
    `count` is the number of words to skip.
    """
    # Simply call the tool and return the new cursor
    return nb_skip_n_words(buffer, start_cursor, end_cursor, count)


@app_njit()
def nb_skip_words_parser_stage(out_b0, f_state, n, count):
    nb_stage_loop_a1(nb_skip_words_parser, out_b0, f_state, n, count)
