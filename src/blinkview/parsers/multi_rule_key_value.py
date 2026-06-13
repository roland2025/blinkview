# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace
from typing import Callable

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable, configuration_property, override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.factory import BaseFactory
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.system_context import SystemContext
from blinkview.parsers.parser import BaseParser, ParserFactory
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

# =============================================================================
# TYPE ALIAS
# ProcessFn signature:
#   (b_in, i, module_id, batch) -> None
#
# All batch threshold and flush mechanics are evaluated directly within the
# parser hot loop prior to calling closures, keeping match processors clean.
# =============================================================================
ProcessFn = Callable


@configurable
@bindable
@configuration_property("enabled", type="boolean", default=True, ui_order=2, title="Enabled")
class ExtractionRule:
    """Base class for section-based polymorphic log extraction rules."""

    shared: SystemContext
    local: SimpleNamespace

    module_name: str
    module_suffix: str

    def bundle(self) -> tuple[int, ProcessFn]:
        """Returns a tuple of (base_module_id, process_fn).

        process_fn signature:
            (b_in, i, module_id, batch) -> None
        """
        raise NotImplementedError


class ExtractionRuleFactory(BaseFactory[ExtractionRule]):
    pass


# =============================================================================
# DELIMITER EXTRACTION RULE
# =============================================================================
@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", default="", ui_order=10, title="Module Name Suffix")
@configuration_property(
    "prefix_strip", type="string", default="", ui_order=12, title="Prefix to Strip (e.g., 'Data: ')"
)
@configuration_property("field_delimiter", type="string", default="&", ui_order=15, title="Field Delimiter")
@configuration_property("kv_delimiter", type="string", default="=", ui_order=20, title="Key-Value Delimiter")
@ExtractionRuleFactory.register("key_value")
class DelimiterExtractionRule(ExtractionRule):
    """Extracts key=value pairs with a delimiter."""

    prefix_strip: str
    field_delimiter: str
    kv_delimiter: str

    def bundle(self) -> tuple[int, ProcessFn]:
        _resolve_module = self.shared.id_registry.resolve_module
        _get_module = self.local.device_id.get_module
        device_id_int = self.local.device_id.id

        # 1. Resolve parent module metadata ONCE at bundle time
        parent_mod = _resolve_module(self.module_name)
        base_mod_id = parent_mod.id
        parent_name = parent_mod.name

        field_delim = getattr(self, "field_delimiter", "&").encode("ascii")
        kv_delim = getattr(self, "kv_delimiter", "=").encode("ascii")
        suffix = getattr(self, "module_suffix", "").strip() or None
        prefix_bytes = getattr(self, "prefix_strip", "").encode("ascii") or None

        field_delim_int = field_delim[0] if len(field_delim) == 1 else 38
        kv_delim_int = kv_delim[0] if len(kv_delim) == 1 else 61

        name_prefix = f"{suffix}." if suffix else f"{parent_name}."

        _len = len
        _range = range

        # Isolated flat cache: Key is raw `k_bytes` -> Value is target_mod_id
        module_cache = {}
        flat_cache = []

        local_ctx = self.local.parser_local

        def process(i, module_id, batch):
            buffer = local_ctx.buffer_mv

            start = local_ctx.offsets_mv[i]
            end = start + local_ctx.lengths_mv[i]

            if prefix_bytes:
                p_len = _len(prefix_bytes)
                if (end - start) >= p_len:
                    match = True
                    for idx in _range(p_len):
                        if buffer[start + idx] != prefix_bytes[idx]:
                            match = False
                            break
                    if match:
                        start += p_len

            ts_ns = local_ctx.timestamps_mv[i]
            rx_ns = local_ctx.rx_timestamps_mv[i]
            level = local_ctx.levels_mv[i]

            batch_insert = batch.insert

            chunk_start = start
            kv_pos = -1

            for j in _range(start, end + 1):
                if j < end:
                    c = buffer[j]
                else:
                    c = field_delim_int

                if c == kv_delim_int and kv_pos == -1 and j < end:
                    kv_pos = j
                elif c == field_delim_int:
                    if kv_pos != -1:
                        k_start = chunk_start
                        k_end = kv_pos
                        while k_start < k_end and buffer[k_start] in (32, 9, 10, 13):
                            k_start += 1
                        while k_end > k_start and buffer[k_end - 1] in (32, 9, 10, 13):
                            k_end -= 1

                        v_start = kv_pos + 1
                        v_end = j
                        while v_start < v_end and buffer[v_start] in (32, 9, 10, 13):
                            v_start += 1
                        while v_end > v_start and buffer[v_end - 1] in (32, 9, 10, 13):
                            v_end -= 1

                        if k_start < k_end and v_start < v_end:
                            k_view = buffer[k_start:k_end]
                            v_view = buffer[v_start:v_end]

                            target_mod_id = None
                            for kb, mid in flat_cache:
                                if k_view == kb:
                                    target_mod_id = mid
                                    break

                            # 3. CACHE MISS (Runs exactly once per unique field name encountered)
                            if target_mod_id is None:
                                try:
                                    k_bytes = k_view.tobytes()  # Allocate the permanent key wrapper
                                    key_str = k_bytes.decode("ascii", errors="ignore")
                                    target_mod_id = _get_module(f"{name_prefix}{key_str}").id

                                    # Store permanent bytes object and ID in flat lookup list
                                    flat_cache.append((k_bytes, target_mod_id))
                                except Exception:
                                    pass
                            if target_mod_id is not None:
                                try:
                                    batch_insert(
                                        ts_ns=ts_ns,
                                        rx_ts_ns=rx_ns,
                                        msg_bytes=v_view,
                                        level=level,
                                        module=target_mod_id,
                                        device=device_id_int,
                                    )
                                except Exception:
                                    pass

                    chunk_start = j + 1
                    kv_pos = -1

        return base_mod_id, process


# =============================================================================
# TOKEN EXTRACTION RULE
# =============================================================================
@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", required=True, default="", ui_order=10, title="New module name")
@configuration_property(
    "token_match_type",
    type="string",
    enum=["starts_with", "contains", "ends_with"],
    required=True,
    default="contains",
    ui_order=15,
)
@configuration_property(
    "token_pattern", type="string", required=True, default="", ui_order=20, title="Token Pattern Anchor"
)
@configuration_property(
    "token_start_index", type="integer", required=True, default=1, ui_order=25, title="Word Start Index"
)
@configuration_property(
    "token_word_count", type="integer", required=False, default=1, ui_order=30, title="Word Count (0 = All)"
)
@ExtractionRuleFactory.register("token")
class TokenExtractionRule(ExtractionRule):
    """Extract values matching startswith/endswith/contains patterns."""

    token_match_type: str
    token_pattern: str
    token_start_index: int
    token_word_count: int

    def bundle(self) -> tuple[int, ProcessFn]:
        _resolve_module = self.shared.id_registry.resolve_module
        _module_from_int = self.shared.id_registry.module_from_int
        _get_module = self.local.device_id.get_module
        device_id_int = self.local.device_id.id

        base_mod_id = _resolve_module(self.module_name).id

        pattern_bytes = getattr(self, "token_pattern", "").encode("ascii")
        match_type = getattr(self, "token_match_type", "contains")
        z_start = getattr(self, "token_start_index", 0)
        y_count = getattr(self, "token_word_count", 1)
        suffix = getattr(self, "module_suffix", "").strip() or None
        static_target_id = None

        module_cache = {}

        if suffix:
            try:
                static_target_id = _get_module(suffix).id
            except Exception:
                pass

        pat_len = len(pattern_bytes)
        _len = len
        _range = range

        if match_type == "starts_with":

            def _match(buf, start, end) -> bool:
                if (end - start) < pat_len:
                    return False
                for idx in _range(pat_len):
                    if buf[start + idx] != pattern_bytes[idx]:
                        return False
                return True

        elif match_type == "ends_with":

            def _match(buf, start, end) -> bool:
                if (end - start) < pat_len:
                    return False
                offset = end - pat_len
                for idx in _range(pat_len):
                    if buf[offset + idx] != pattern_bytes[idx]:
                        return False
                return True

        else:  # contains

            def _match(buf, start, end) -> bool:
                if (end - start) < pat_len:
                    return False

                # Cache the head byte to build an inline high-speed rejection filter
                first_byte = pattern_bytes[0]

                for idx in _range(start, end - pat_len + 1):
                    # Cheap primitive check skips the heavy inner evaluation frame
                    if buf[idx] != first_byte:
                        continue

                    match_found = True
                    # Start scanning from index 1 since index 0 is already validated
                    for j in _range(1, pat_len):
                        if buf[idx + j] != pattern_bytes[j]:
                            match_found = False
                            break
                    if match_found:
                        return True
                return False

        local_ctx = self.local.parser_local

        def process(i, module_id, batch):
            buffer = local_ctx.buffer_mv

            start = local_ctx.offsets_mv[i]
            end = start + local_ctx.lengths_mv[i]

            if not _match(buffer, start, end):
                return

            start_byte = -1
            end_byte = -1
            word_count = 0
            in_word = False

            for j in _range(start, end):
                c = buffer[j]
                is_ws = c == 32 or c == 9 or c == 10 or c == 13

                if not in_word:
                    if not is_ws:
                        in_word = True
                        if word_count == z_start:
                            start_byte = j
                else:
                    if is_ws:
                        in_word = False
                        if y_count > 0 and word_count == z_start + y_count - 1:
                            end_byte = j
                            break
                        if word_count >= z_start:
                            end_byte = j
                        word_count += 1

            if in_word and word_count >= z_start:
                if y_count == 0 or word_count < z_start + y_count:
                    end_byte = end

            if start_byte == -1 or end_byte == -1 or start_byte >= end_byte:
                return

            v_view = buffer[start_byte:end_byte]

            try:
                if static_target_id is not None:
                    target_mod_id = static_target_id
                else:
                    cache_key = (module_id, z_start, y_count)

                    try:
                        target_mod_id = module_cache[cache_key]
                    except KeyError:
                        parent_name = _module_from_int(module_id).name
                        pattern_str = pattern_bytes.decode("ascii", errors="ignore")
                        sub_mod_name = f"{parent_name}.{pattern_str}_z{z_start}"
                        target_mod_id = _get_module(sub_mod_name).id
                        module_cache[cache_key] = target_mod_id

                batch.insert(
                    ts_ns=local_ctx.timestamps_mv[i],
                    rx_ts_ns=local_ctx.rx_timestamps_mv[i],
                    msg_bytes=v_view,
                    level=local_ctx.levels_mv[i],
                    module=target_mod_id,
                    device=device_id_int,
                )
            except Exception:
                pass

        return base_mod_id, process


# =============================================================================
# JSON-LITE EXTRACTION RULE
# =============================================================================
@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", default="", ui_order=10, title="New Module Name")
@configuration_property("json_key", type="string", default="", required=True, ui_order=15, title="JSON Key to Extract")
class JsonLiteExtractionRule(ExtractionRule):
    json_key: str

    def bundle(self) -> tuple[int, ProcessFn]:
        _get_module = self.local.device_id.get_module
        _module_from_int = self.shared.id_registry.module_from_int
        device_id_int = self.local.device_id.id

        base_mod_id = _get_module(self.module_name).id
        suffix = getattr(self, "module_suffix", "").strip() or None
        static_target_id = None

        if suffix:
            try:
                static_target_id = _get_module(f"{self.module_name}.{suffix}").id
            except Exception:
                pass

        json_key_raw = getattr(self, "json_key", "").strip()
        json_key_bytes = f'"{json_key_raw}":'.encode("ascii") if json_key_raw else b""
        key_len = len(json_key_bytes)
        key_str_clean = json_key_raw

        _len = len
        module_cache = {}

        if not json_key_bytes:

            def process(i, module_id, batch):
                pass

            return base_mod_id, process

        _range = range

        local_ctx = self.local.parser_local

        def process(i, module_id, batch):
            buffer = local_ctx.buffer_mv

            start = local_ctx.offsets_mv[i]
            end = start + local_ctx.lengths_mv[i]
            msg_len = end - start

            if msg_len < key_len:
                return

            # Zero-copy primitive scan over the fast memoryview layout
            idx = -1
            for match_idx in _range(start, end - key_len + 1):
                found = True
                for k in _range(key_len):
                    if buffer[match_idx + k] != json_key_bytes[k]:
                        found = False
                        break
                if found:
                    idx = match_idx
                    break

            if idx == -1:
                return

            ts_ns = local_ctx.timestamps_mv[i]
            level = local_ctx.levels_mv[i]

            pos = idx + key_len

            while pos < end and buffer[pos] == 32:
                pos += 1
            if pos >= end:
                return

            # String or numerical/boolean boundaries tracking
            if buffer[pos] == 34:  # ASCII for '"'
                pos += 1
                end_pos = -1
                for scan_pos in _range(pos, end):
                    if buffer[scan_pos] == 34:
                        end_pos = scan_pos
                        break
                if end_pos == -1:
                    return
                v_bytes = buffer[pos:end_pos]
            else:
                end_pos = pos
                while end_pos < end and buffer[end_pos] not in (44, 125, 93, 32, 10, 13):
                    end_pos += 1
                v_bytes = buffer[pos:end_pos]

            if not v_bytes:
                return

            try:
                if static_target_id is not None:
                    target_mod_id = static_target_id
                else:
                    if module_id not in module_cache:
                        parent_name = _module_from_int(module_id).name
                        module_cache[module_id] = _get_module(f"{parent_name}.{key_str_clean}").id
                    target_mod_id = module_cache[module_id]

                batch.insert(
                    ts_ns=ts_ns,
                    rx_ts_ns=ts_ns,
                    msg_bytes=v_bytes,
                    level=level,
                    module=target_mod_id,
                    device=device_id_int,
                )
            except Exception:
                pass

        return base_mod_id, process


# =============================================================================
# LIST DELIMITER EXTRACTION RULE
# =============================================================================
@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", default="", ui_order=10, title="New Module Name")
@configuration_property("startswith", type="string", default="", ui_order=8, title="Must start with signature")
@configuration_property("prefix_strip", type="string", default="", ui_order=10, title="Prefix to Strip")
@configuration_property("field_delimiter", type="string", default=";", ui_order=15, title="Field Delimiter")
@configuration_property(
    "field_names",
    type="array",
    required=True,
    ui_order=20,
    title="Field Names by Position",
    items={
        "type": "object",
        "title": "Field Mapping",
        "required": ["name"],
        "properties": {
            "name": {"type": "string", "title": "Field Name", "default": ""},
            "ignore": {"type": "boolean", "title": "Ignore Field", "default": False},
        },
    },
)
@ExtractionRuleFactory.register("list_delimiter")
class ListDelimiterExtractionRule(ExtractionRule):
    """Extract from CSV-like formats using object-mapped positional schema rules."""

    startswith: str
    prefix_strip: str
    field_delimiter: str
    field_names: list

    def bundle(self) -> tuple[int, ProcessFn]:
        _resolve_module = self.shared.id_registry.resolve_module
        _get_module = self.local.device_id.get_module
        device_id_int = self.local.device_id.id

        parent_mod = _resolve_module(self.module_name)
        base_mod_id = parent_mod.id

        suffix = getattr(self, "module_suffix", "").strip()
        if suffix:
            try:
                parent_mod = _get_module(suffix)
            except Exception:
                pass

        field_delim = getattr(self, "field_delimiter", ";").encode("ascii")
        prefix_bytes = getattr(self, "prefix_strip", "").encode("ascii") or None
        start_bytes = getattr(self, "startswith", "").strip().encode("ascii") or None
        field_delim_int = field_delim[0] if len(field_delim) == 1 else 59

        static_target_ids = []
        for field_cfg in getattr(self, "field_names", []):
            if not field_cfg:
                static_target_ids.append(None)
                continue
            get_fval = (
                (lambda k, d: field_cfg.get(k, d))
                if isinstance(field_cfg, dict)
                else (lambda k, d: getattr(field_cfg, k, d))
            )
            ignore = get_fval("ignore", False)
            name = get_fval("name", "").strip()
            if ignore or not name:
                static_target_ids.append(None)
                continue
            try:
                static_target_ids.append(_get_module(f"{parent_mod.name}.{name}").id)
            except Exception:
                static_target_ids.append(None)

        n_fields = len(static_target_ids)
        start_len = len(start_bytes) if start_bytes else 0
        prefix_len = len(prefix_bytes) if prefix_bytes else 0
        _len = len

        _range = range

        local_ctx = self.local.parser_local

        def process(i, module_id, batch):
            buffer = local_ctx.buffer_mv

            start = local_ctx.offsets_mv[i]
            end = start + local_ctx.lengths_mv[i]

            msg_len = end - start
            scan_start = start

            if start_bytes:
                if msg_len < start_len:
                    return
                for j in _range(start_len):
                    if buffer[start + j] != start_bytes[j]:
                        return
                scan_start = start + start_len

            if prefix_bytes:
                if (end - scan_start) >= prefix_len:
                    has_prefix = True
                    for j in _range(prefix_len):
                        if buffer[scan_start + j] != prefix_bytes[j]:
                            has_prefix = False
                            break
                    if has_prefix:
                        scan_start += prefix_len

            field_idx = 0
            field_start = scan_start

            ts_ns = local_ctx.timestamps_mv[i]
            rx_ns = local_ctx.rx_timestamps_mv[i]
            level = local_ctx.levels_mv[i]

            for j in _range(scan_start, end + 1):
                is_delim = False
                if j < end:
                    if buffer[j] == field_delim_int:
                        is_delim = True
                else:
                    is_delim = True

                if is_delim:
                    target_mod_id = static_target_ids[field_idx]

                    if target_mod_id is not None:
                        chunk_start = field_start
                        chunk_end = j

                        # Inline zero-allocation stripping loops
                        while chunk_start < chunk_end and buffer[chunk_start] in (32, 9, 10, 13):
                            chunk_start += 1
                        while chunk_end > chunk_start and buffer[chunk_end - 1] in (32, 9, 10, 13):
                            chunk_end -= 1

                        if chunk_start < chunk_end:
                            v_view = buffer[chunk_start:chunk_end]

                            try:
                                batch.insert(
                                    ts_ns=ts_ns,
                                    rx_ts_ns=rx_ns,
                                    msg_bytes=v_view,
                                    level=level,
                                    module=target_mod_id,
                                    device=device_id_int,
                                )
                            except Exception:
                                pass

                    field_idx += 1
                    if field_idx >= n_fields:
                        break
                    field_start = j + 1

        return base_mod_id, process


# =============================================================================
# PURE WORD INDEX EXTRACTION RULE
# =============================================================================
@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", required=True, ui_order=10, title="New Module Name")
@configuration_property("word_index", type="integer", default=0, ui_order=15, title="Word Index (Z)")
@configuration_property("word_count", type="integer", default=1, ui_order=20, title="Word Count (0 = All remaining)")
@ExtractionRuleFactory.register("word")
class WordExtractionRule(ExtractionRule):
    """Extracts a specific word or slice of words directly by its whitespace-split index."""

    word_index: int
    word_count: int

    def bundle(self) -> tuple[int, ProcessFn]:
        _resolve_module = self.shared.id_registry.resolve_module
        _get_module = self.local.device_id.get_module
        device_id_int = self.local.device_id.id

        base_mod_id = _resolve_module(self.module_name).id
        suffix = self.module_suffix.strip()
        static_target_id = _get_module(suffix).id

        z_start = getattr(self, "word_index", 0)
        y_count = getattr(self, "word_count", 1)
        _len = len

        local_ctx = self.local.parser_local

        _range = range

        def process(i, module_id, batch):
            buffer = local_ctx.buffer_mv

            start = local_ctx.offsets_mv[i]
            end = start + local_ctx.lengths_mv[i]

            start_byte = -1
            end_byte = -1
            word_count = 0
            in_word = False

            for j in _range(start, end):
                c = buffer[j]
                is_ws = c == 32 or c == 9 or c == 10 or c == 13

                if not in_word:
                    if not is_ws:
                        in_word = True
                        if word_count == z_start:
                            start_byte = j
                else:
                    if is_ws:
                        in_word = False
                        if y_count > 0 and word_count == z_start + y_count - 1:
                            end_byte = j
                            break
                        if word_count >= z_start:
                            end_byte = j
                        word_count += 1

            if in_word:
                if word_count >= z_start:
                    if y_count == 0 or word_count < z_start + y_count:
                        end_byte = end

            if start_byte == -1 or end_byte == -1 or start_byte >= end_byte:
                return

            v_view = buffer[start_byte:end_byte]

            try:
                batch.insert(
                    ts_ns=local_ctx.timestamps_mv[i],
                    rx_ts_ns=local_ctx.rx_timestamps_mv[i],
                    msg_bytes=v_view,
                    level=local_ctx.levels_mv[i],
                    module=static_target_id,
                    device=device_id_int,
                )
            except Exception:
                pass

        return base_mod_id, process


# =============================================================================
# PARSER
# =============================================================================
@configuration_property(
    "rules",
    type="array",
    required=True,
    ui_order=10,
    title="Polymorphic Parsing Rules",
    items={
        "type": "object",
        "_factory": "key_value_rule",
        "title": "Extraction Rule Setup",
    },
)
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/targets"},
)
@ParserFactory.register("multi_rule_key_value")
class MultiRuleKeyValueParser(BaseParser):
    rules: list

    def __init__(self):
        super().__init__()
        self._instantiated_rules = []

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        self._instantiated_rules = []
        factory_build = self.shared.factories.build
        local_ctx = SimpleNamespace(device_id=self.local.device_id, parser_local=self.local)

        for rule_cfg in config.get("rules", []):
            rule_obj = factory_build("key_value_rule", rule_cfg, system_ctx=self.shared, local_ctx=local_ctx)
            self._instantiated_rules.append(rule_obj)

        return changed

    def run(self):
        # --- Localize Framework Plumbing ---
        _time_ns = self.shared.time_ns
        _get = self.input_queue.get
        _distribute = self.distribute

        _range = range
        _len = len

        max_timeout = self.delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)

        device_identity = self.local.device_id
        device_identity_id = device_identity.id
        system_identity_id = self.shared.id_registry.get_device("SYSTEM").id

        logger = self.logger

        # --- Compile Rules → callable map ---
        rules_by_module: dict[int, list[ProcessFn]] = {}

        for rule in self._instantiated_rules:
            if not getattr(rule, "enabled", True):
                logger.info(f"Skipping disabled rule: '{getattr(rule, 'module_name', 'unknown')}'")
                continue

            base_mod_id, process_fn = rule.bundle()
            logger.debug(f"Compiled rule for module_id={base_mod_id}: {rule.__class__.__name__}")

            if base_mod_id not in rules_by_module:
                rules_by_module[base_mod_id] = []
            rules_by_module[base_mod_id].append(process_fn)

        # Find the maximum module ID present in your rules to size our list
        max_mod_id = max(rules_by_module.keys()) if rules_by_module else 0

        # Allocate a flat array initialized to None
        rules_flat_list = [None] * (max_mod_id + 1)
        for mod_id, process_fns in rules_by_module.items():
            rules_flat_list[mod_id] = process_fns

        # Localize the list and its length for zero-overhead boundary checking
        rules_flat_list_len = _len(rules_flat_list)

        del rules_by_module

        # --- Auto-Tuning Trackers ---
        speed_out = Speedometer(logger=self.logger.child("stats_out"))
        tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

        # --- Batch Pool ---
        pool_create = self.shared.array_pool.create

        def batch_acquire():
            return pool_create(
                PooledLogBatch,
                tuner_out.estimated_capacity,
                tuner_out.estimated_buffer_bytes,
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        # Flat structural variable tracking instead of tracking an allocation array
        batch_out = batch_acquire()
        batch_out_time = _time_ns()
        batch_out_capacity = batch_out.capacity
        batch_out_buf_limit = batch_out.buffer_capacity() * 0.9

        def flush():
            nonlocal batch_out, batch_out_time, batch_out_capacity, batch_out_buf_limit
            if batch_out and batch_out.size > 0:
                with batch_out:
                    tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=max_timeout)
                    _distribute(batch_out)
            batch_out = batch_acquire()
            batch_out_time = _time_ns()
            batch_out_capacity = batch_out.capacity
            batch_out_buf_limit = batch_out.buffer_capacity() * 0.9

        stop_is_set = self._stop_event.is_set

        local_space = self.local

        # --- Hot Loop ---
        while not stop_is_set():
            now_ns = _time_ns()

            if batch_out.size > 0:
                elapsed_ns = now_ns - batch_out_time
                current_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
            else:
                current_timeout = 120.0

            batch_in = _get(timeout=current_timeout)

            if not batch_in:
                if batch_out.size > 0:
                    flush()
                continue

            with batch_in:
                try:
                    b_in = batch_in.bundle
                    if b_in is None:
                        continue

                    count = batch_in.size
                    if count == 0:
                        continue

                    if batch_out.size == 0:
                        batch_out_time = _time_ns()

                    # Create zero-allocation memoryviews ONCE per batch
                    modules_mv = memoryview(b_in.modules)
                    devices_mv = memoryview(b_in.devices)

                    # Map metadata arrays to the shared local context for the rules
                    local_space.buffer_mv = memoryview(b_in.buffer)
                    local_space.offsets_mv = memoryview(b_in.offsets)
                    local_space.lengths_mv = memoryview(b_in.lengths)
                    local_space.timestamps_mv = memoryview(b_in.timestamps)
                    local_space.rx_timestamps_mv = memoryview(b_in.rx_timestamps)
                    local_space.levels_mv = memoryview(b_in.levels)

                    for i in _range(count):
                        device_id = devices_mv[i]
                        if device_id == device_identity_id or device_id == system_identity_id:
                            continue  # The for loop automatically advances 'i' safely!

                        module_id = modules_mv[i]
                        if module_id < rules_flat_list_len:
                            rules = rules_flat_list[module_id]

                            if rules is not None:
                                if batch_out.size >= batch_out_capacity or batch_out.msg_cursor >= batch_out_buf_limit:
                                    flush()

                                num_rules = _len(rules)
                                if num_rules == 1:
                                    rules[0](i, module_id, batch_out)
                                elif num_rules == 2:
                                    # Hard-coded execution vectoring
                                    rules[0](i, module_id, batch_out)
                                    rules[1](i, module_id, batch_out)
                                elif num_rules == 3:
                                    rules[0](i, module_id, batch_out)
                                    rules[1](i, module_id, batch_out)
                                    rules[2](i, module_id, batch_out)
                                else:
                                    # Zero-allocation manual stack-counter for multi-rule edge-cases
                                    r_idx = 0
                                    while r_idx < num_rules:
                                        rules[r_idx](i, module_id, batch_out)
                                        r_idx += 1
                        i += 1
                except Exception as e:
                    logger.exception("Poison batch encountered, skipping remainder.", e)

            if batch_out.size > 0 and (_time_ns() - batch_out_time >= max_timeout_ns):
                flush()

        # Drain on shutdown
        if batch_out:
            if batch_out.size > 0:
                flush()
            else:
                batch_out.release()
