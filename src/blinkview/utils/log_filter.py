# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import shlex
from typing import Optional

from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.ops.kv_filter import KvConditionArrays, build_kv_condition_arrays
from blinkview.utils.log_level import LevelIdentity, LogLevel


class LogFilter:
    def __init__(
        self, id_registry, allowed_device=None, filtered_module=None, log_level=None, filtered_module_children=False
    ):
        self.registry = id_registry

        self.filter_index = None

        self.allowed_device: Optional[DeviceIdentity] = id_registry.resolve_device(allowed_device)
        self.filtered_module: Optional[ModuleIdentity] = id_registry.resolve_module(filtered_module)
        self.filtered_module_children = filtered_module_children
        self.log_level: Optional[LevelIdentity] = LogLevel.from_string(log_level, LogLevel.ALL)

        self.kv_filter_text = ""
        self.kv_conditions: list[tuple[bytes, bytes]] = []
        self._kv_baked_key = None
        self._kv_baked_arrays: Optional[KvConditionArrays] = None

    def set_level(self, log_level):
        self.log_level = LogLevel.from_string(log_level)

    def set_kv_filter(self, text: str):
        """Parses a logfmt-syntax query (e.g. `status=ok user_id=42`) into ANDed key=value
        conditions for the row-level Numba filter kernels. Quoted values (`msg="hello world"`)
        may contain spaces; malformed quoting is ignored gracefully (falls back to no filter)."""
        self.kv_filter_text = text or ""
        self.kv_conditions = self._parse_kv_conditions(self.kv_filter_text)

    @staticmethod
    def is_kv_query_ready(text: str) -> bool:
        """False only while the user has just typed a trailing "key=" with no value character
        yet - lets callers (the debounced toolbar field) hold off reloading mid-pair instead of
        re-fetching against a query that's guaranteed to drop every row's value."""
        tokens = (text or "").split()
        if not tokens:
            return True
        return not tokens[-1].endswith("=")

    @staticmethod
    def _parse_kv_conditions(text: str) -> list[tuple[bytes, bytes]]:
        text = text.strip()
        if not text:
            return []

        try:
            tokens = shlex.split(text)
        except ValueError:
            # Unbalanced quotes etc. - treat as "no filter" rather than raising into the UI.
            return []

        conditions = []
        for token in tokens:
            key, sep, value = token.partition("=")
            key = key.strip()
            value = value.strip()
            if not sep or not key:
                continue
            conditions.append((key.encode("utf-8"), value.encode("utf-8")))

        return conditions

    def bake_kv_arrays(self) -> KvConditionArrays:
        """Flattens kv_conditions into the flat arrays the Numba filter kernels expect, caching
        the result until kv_conditions changes - same "bake once, reuse until changed" pattern
        as the effective_mask caches in LogViewerWidget/LogTableModel."""
        cache_key = tuple(self.kv_conditions)
        if self._kv_baked_key != cache_key or self._kv_baked_arrays is None:
            self._kv_baked_key = cache_key
            self._kv_baked_arrays = build_kv_condition_arrays(self.kv_conditions)
        return self._kv_baked_arrays
