# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Shared types for the Numba-backed multi-rule key-value extractor (ops/kv_extraction.py,
parsers/multi_rule_key_value.py). See plans/kv-extractor-numba-backend.md.

Every rule type funnels through one flat `KvRuleConfig` shape (unused fields left at their
default), the same "one config shape fits every step" convention already used by
`UnifiedParserConfig` (core/types/parsing.py) for the frame-parser pipeline - not because every
rule needs every field, but because a `numba.typed.List` needs one homogeneous element type, and
inventing a per-rule-type tuple shape would mean a different list (and a different dispatch
kernel signature) per rule type instead of one.
"""

from typing import NamedTuple

import numpy as np

from blinkview.core.types.empty import EMPTY_BYTES_RO, EMPTY_ID
from blinkview.core.types.modules import MODULE_ID_UNKNOWN


class KvRuleID:
    KEY_VALUE = 0
    ANCHOR_WORD = 1
    JSON_LITE = 2
    DSV = 3
    POSITIONAL = 4


# Match modes for ANCHOR_WORD's pattern test - mirrors AnchorWordExtractionRule.match's enum.
KV_MATCH_STARTS_WITH = 0
KV_MATCH_ENDS_WITH = 1
KV_MATCH_CONTAINS = 2


class KvRuleConfig(NamedTuple):
    # KEY_VALUE + DSV: byte to split fields on (' ' / ';' / ...)
    field_delim: int = 32
    # KEY_VALUE: byte separating key from value ('=' by default)
    kv_delim: int = 61
    # KEY_VALUE: literal prefix to strip before scanning (e.g. "Data: "); DSV: same, applied after
    # any `pattern_bytes` "startswith" signature is consumed
    prefix_bytes: np.ndarray = EMPTY_BYTES_RO
    # KEY_VALUE only: "parent." or "suffix." bytes prepended (lowercase) to an extracted key
    # before hashing it against the module registry - see nb_write_prefixed_key_lower.
    name_prefix_bytes: np.ndarray = EMPTY_BYTES_RO
    # ANCHOR_WORD: the pattern to match against; JSON_LITE: the `"key":` search bytes;
    # DSV: the optional "startswith" signature bytes.
    pattern_bytes: np.ndarray = EMPTY_BYTES_RO
    # ANCHOR_WORD only: KV_MATCH_* - which of starts/ends/contains to test pattern_bytes against.
    match_mode: int = KV_MATCH_CONTAINS
    # ANCHOR_WORD + POSITIONAL: whitespace-split word index to start extracting from.
    word_index: int = 0
    # ANCHOR_WORD + POSITIONAL: number of words to extract (0 = all remaining).
    word_count: int = 0
    # ANCHOR_WORD + JSON_LITE + POSITIONAL: the single resolved target module id (resolved once,
    # at rule-bundle time in Python - none of these three ever need a per-row dynamic module
    # lookup, since their target module name is fully determined by static rule config, not by
    # extracted row content). MODULE_ID_UNKNOWN means "nothing configured, skip".
    static_target_id: int = MODULE_ID_UNKNOWN
    # DSV only: one resolved target module id per delimited field position (parallel array,
    # MODULE_ID_UNKNOWN entries are ignored/skipped fields).
    field_target_ids: np.ndarray = EMPTY_ID


EmptyKvRuleConfig = KvRuleConfig()


class KvExtractState(NamedTuple):
    # 1-element int64 array: resume cursor into the current input batch. Lets
    # nb_process_kv_batch be called repeatedly against the same input batch (once per output
    # batch it fills), the same resumable-chunk contract nb_process_batch_kernel already uses -
    # see ops/dispatch.py / parsers/binary_parser.py's run() loop.
    in_idx: np.ndarray
