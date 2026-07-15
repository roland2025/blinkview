# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core.dtypes import BYTE
from blinkview.core.numba_config import app_njit
from blinkview.ops.strings import nb_to_lower

EMPTY_TEXT_BYTES = np.frombuffer(b"", dtype=BYTE)  # read-only, matching build_text_search_arrays' real needle_buf
EMPTY_BOOL_MASK = np.empty(0, dtype=np.bool_)


class TextSearchArrays(NamedTuple):
    """Flattened free-text search query, ready to pass into the Numba filter kernels."""

    needle_buf: np.ndarray
    needle_len: int
    dev_mask: np.ndarray  # bool, indexed by device id - True if that device's name matches
    mod_mask: np.ndarray  # bool, indexed by module id - True if that module's name matches


EMPTY_TEXT_SEARCH = TextSearchArrays(EMPTY_TEXT_BYTES, 0, EMPTY_BOOL_MASK, EMPTY_BOOL_MASK)


def build_text_search_arrays(text: str, devices: dict, modules: dict) -> TextSearchArrays:
    """Bakes a free-text query into the flat arrays the Numba filter kernels expect: a
    lowercased needle for message-body substring search, plus boolean masks (indexed by
    device/module id, built from the id_registry's name dicts) marking which devices/modules
    have a matching name - so "filter device/module/message" semantics are preserved even
    though the scan now runs inside the Numba fetch kernels against the full backend, instead
    of a Qt proxy limited to whatever rows happened to already be fetched. Pure Python - this
    runs once per filter-text change, not per row."""
    text = (text or "").strip().lower()
    if not text:
        return EMPTY_TEXT_SEARCH

    needle_bytes = text.encode("utf-8")

    max_dev_id = max(devices.keys(), default=-1)
    dev_mask = np.zeros(max_dev_id + 1, dtype=np.bool_)
    for dev_id, identity in devices.items():
        if text in identity.name.lower():
            dev_mask[dev_id] = True

    max_mod_id = max(modules.keys(), default=-1)
    mod_mask = np.zeros(max_mod_id + 1, dtype=np.bool_)
    for mod_id, identity in modules.items():
        if text in identity.name.lower():
            mod_mask[mod_id] = True

    return TextSearchArrays(
        needle_buf=np.frombuffer(needle_bytes, dtype=BYTE) if needle_bytes else EMPTY_TEXT_BYTES,
        needle_len=len(needle_bytes),
        dev_mask=dev_mask,
        mod_mask=mod_mask,
    )


@app_njit(inline="always")
def nb_bytes_contains_ci(haystack, start, length, needle, needle_len) -> bool:
    """Case-insensitive substring search over haystack[start:start+length]. `needle` must
    already be lowercased ASCII bytes (as produced by build_text_search_arrays)."""
    if needle_len == 0:
        return True
    if needle_len > length:
        return False

    last_start = start + length - needle_len
    for i in range(start, last_start + 1):
        matched = True
        for j in range(needle_len):
            if nb_to_lower(haystack[i + j]) != needle[j]:
                matched = False
                break
        if matched:
            return True
    return False
