# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import shlex
from typing import Optional

from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.ops.id_history import EMPTY_ID_HISTORY_TABLE
from blinkview.ops.kv_filter import KvConditionArrays, build_kv_condition_arrays
from blinkview.ops.process_filter import EMPTY_PROC_FILTER, ProcessFilter, build_process_filter
from blinkview.ops.text_filter import TextSearchArrays, build_text_search_arrays
from blinkview.utils.log_level import LevelIdentity, LogLevel

# The kv-box key that filters on a row's process name (pid_history) instead of a key in the
# message - matched case-insensitively, value as a case-insensitive substring.
PROCESS_KV_KEY = "process"


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

        self.text_filter_text = ""
        self._text_baked_key = None
        self._text_baked_arrays: Optional[TextSearchArrays] = None

        # `process=` values from the kv box, lowercased; ANDed like the other conditions.
        self.process_terms: list[str] = []
        self._proc_baked_key = None
        self._proc_baked: ProcessFilter = EMPTY_PROC_FILTER

    def set_level(self, log_level):
        self.log_level = LogLevel.from_string(log_level)

    def set_kv_filter(self, text: str):
        """Parses a logfmt-syntax query (e.g. `status=ok user_id=42`) into ANDed key=value
        conditions for the row-level Numba filter kernels. Quoted values (`msg="hello world"`)
        may contain spaces; malformed quoting is ignored gracefully (falls back to no filter).

        `process=<name>` is not a message key: it keeps rows whose process (resolved from
        pid_history, see bake_process_filter) contains <name>, and is left out of kv_conditions."""
        self.kv_filter_text = text or ""
        conditions = self._parse_kv_conditions(self.kv_filter_text)
        process_key = PROCESS_KV_KEY.encode()
        self.process_terms = [v.decode("utf-8").lower() for k, v in conditions if k.lower() == process_key and v]
        self.kv_conditions = [(k, v) for k, v in conditions if k.lower() != process_key]

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
        as the effective_mask caches in LogViewerWidget/LogTableStore."""
        cache_key = tuple(self.kv_conditions)
        if self._kv_baked_key != cache_key or self._kv_baked_arrays is None:
            self._kv_baked_key = cache_key
            self._kv_baked_arrays = build_kv_condition_arrays(self.kv_conditions)
        return self._kv_baked_arrays

    def set_text_filter(self, text: str):
        """Sets the free-text "device/module/message" search query. Unlike the old Qt-proxy
        search box, this is baked into the same row-level Numba filter kernels as the kv/level
        filters, so a live-mode fetch actually re-scans the backend for matches and fills the
        viewport with them, instead of filtering whatever small window of rows happened to
        already be fetched."""
        self.text_filter_text = text or ""

    def bake_text_search(self) -> TextSearchArrays:
        """Flattens text_filter_text into the flat arrays/masks the Numba filter kernels expect,
        caching the result until the text changes or the device/module registries grow (a newly
        registered device/module needs a chance to be re-checked against the current query)."""
        cache_key = (self.text_filter_text, len(self.registry.devices), len(self.registry.modules))
        if self._text_baked_key != cache_key or self._text_baked_arrays is None:
            self._text_baked_key = cache_key
            self._text_baked_arrays = build_text_search_arrays(
                self.text_filter_text, self.registry.devices, self.registry.modules
            )
        return self._text_baked_arrays

    def bake_process_filter(self, pid_history) -> ProcessFilter:
        """The ProcessFilter for the current `process=` terms and free-text search, over
        `pid_history` (an IdHistory, or None when the source of rows has none: then a `process=`
        condition matches nothing). EMPTY_PROC_FILTER while neither is set, so callers comparing
        by identity see no change; otherwise cached until the terms, the text or the history
        change (a poll naming a new process must reach the next fetch)."""
        text = self.text_filter_text.strip()
        if not text and not self.process_terms:
            return EMPTY_PROC_FILTER

        version = pid_history.version if pid_history is not None else -1
        cache_key = (tuple(self.process_terms), text, version)
        if self._proc_baked_key != cache_key:
            self._proc_baked_key = cache_key
            if pid_history is None:
                table, names = EMPTY_ID_HISTORY_TABLE, ()
            else:
                table = pid_history.table()
                names = pid_history.names()
            self._proc_baked = build_process_filter(table, names, text, self.process_terms)
        return self._proc_baked
