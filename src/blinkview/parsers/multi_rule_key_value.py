# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable, configuration_property, override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.factory import BaseFactory
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.system_context import SystemContext
from blinkview.parsers.parser import BaseParser, ParserFactory


@configurable
@bindable
@configuration_property("enabled", type="boolean", default=True, ui_order=2, title="Enabled")
class ExtractionRule:
    """Base class for section-based polymorphic log extraction rules."""

    shared: SystemContext
    local: SimpleNamespace

    module_name: str
    module_suffix: str

    def bundle(self) -> tuple:
        """Returns a tuple of (base_module_id, compiled_byte_primitives_namespace)"""
        raise NotImplementedError


class ExtractionRuleFactory(BaseFactory[ExtractionRule]):
    pass


# =============================================================================
# 1. DELIMITER EXTRACTION RULE CLASS
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

    def bundle(self):
        _resolve_module = self.shared.id_registry.resolve_module
        base_mod_id = _resolve_module(self.module_name).id

        print(f"[DelimiterExtractionRule] mod_id={base_mod_id} module_name={self.module_name}")

        field_delim = getattr(self, "field_delimiter", "&").encode("ascii")
        kv_delim = getattr(self, "kv_delimiter", "=").encode("ascii")
        suffix = getattr(self, "module_suffix", "").strip()
        prefix_bytes = getattr(self, "prefix_strip", "").encode("ascii")

        compiled = SimpleNamespace(
            mode="delimiter",
            field_delim=field_delim,
            kv_delim=kv_delim,
            field_delim_int=field_delim[0] if len(field_delim) == 1 else None,
            kv_delim_int=kv_delim[0] if len(kv_delim) == 1 else None,
            prefix_bytes=prefix_bytes if prefix_bytes else None,
            module_suffix=suffix if suffix else None,
            static_target_id=None,
        )
        return base_mod_id, compiled


# =============================================================================
# 2. TOKEN EXTRACTION RULE CLASS
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
    """Extract values matching startswith/endswith/contains patterns"""

    token_match_type: str
    token_pattern: str
    token_start_index: int
    token_word_count: int

    def bundle(self):

        _get_module = self.local.device_id.get_module
        _resolve_module = self.shared.id_registry.resolve_module
        base_mod_id = _resolve_module(self.module_name).id

        suffix = getattr(self, "module_suffix", "").strip()
        static_target_id = None
        if suffix:
            try:
                static_target_id = _get_module(suffix).id
            except Exception:
                pass

        compiled = SimpleNamespace(
            mode="token",
            pattern_bytes=getattr(self, "token_pattern", "").encode("ascii"),
            match_type=getattr(self, "token_match_type", "contains"),
            z_start=getattr(self, "token_start_index", 0),
            y_count=getattr(self, "token_word_count", 1),
            module_suffix=suffix if suffix else None,
            static_target_id=static_target_id,
        )
        return base_mod_id, compiled


# =============================================================================
# 3. JSON-LITE EXTRACTION RULE CLASS
# =============================================================================
@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", default="", ui_order=10, title="New Module Name")
@configuration_property("json_key", type="string", default="", required=True, ui_order=15, title="JSON Key to Extract")
# @ExtractionRuleFactory.register("json_lite")
class JsonLiteExtractionRule(ExtractionRule):
    json_key: str

    def bundle(self):
        _get_module = self.local.device_id.get_module
        base_mod_id = _get_module(self.module_name).id

        suffix = getattr(self, "module_suffix", "").strip()
        static_target_id = None
        if suffix:
            try:
                static_target_id = _get_module(f"{self.module_name}.{suffix}").id
            except Exception:
                pass

        json_key_raw = getattr(self, "json_key", "").strip()
        json_key_bytes = f'"{json_key_raw}":'.encode("ascii") if json_key_raw else b""

        compiled = SimpleNamespace(
            mode="json_lite",
            json_key_bytes=json_key_bytes,
            module_suffix=suffix if suffix else None,
            static_target_id=static_target_id,
        )
        return base_mod_id, compiled


# =============================================================================
# 4. LIST DELIMITER EXTRACTION RULE CLASS
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

    def bundle(self):
        _resolve_module = self.shared.id_registry.resolve_module
        _get_module = self.local.device_id.get_module

        parent_mod = _resolve_module(self.module_name)
        base_mod_id = _resolve_module(self.module_name).id

        suffix = getattr(self, "module_suffix", "").strip()
        if suffix:
            try:
                parent_mod = _get_module(suffix)
            except Exception:
                pass

        field_delim = getattr(self, "field_delimiter", ";").encode("ascii")
        prefix_bytes = getattr(self, "prefix_strip", "").encode("ascii")
        start_bytes = getattr(self, "startswith", "").strip().encode("ascii")

        static_target_ids = []
        for field_cfg in getattr(self, "field_names", []):
            if not field_cfg:
                static_target_ids.append(None)
                continue

            # Safe lookup hook protecting against dynamic dictionary vs object namespace variations
            get_fval = lambda k, d: field_cfg.get(k, d) if isinstance(field_cfg, dict) else getattr(field_cfg, k, d)

            ignore = get_fval("ignore", False)
            name = get_fval("name", "").strip()

            # If explicitly ignored or missing an identity name mapping, skip structural ID resolution
            if ignore or not name:
                static_target_ids.append(None)
                continue

            try:
                full_name = f"{parent_mod.name}.{name}"
                static_target_ids.append(_get_module(full_name).id)
            except Exception:
                static_target_ids.append(None)

        compiled = SimpleNamespace(
            mode="list_delimiter",
            field_delim=field_delim,
            field_delim_int=field_delim[0] if len(field_delim) == 1 else None,
            prefix_bytes=prefix_bytes if prefix_bytes else None,
            start_bytes=start_bytes if start_bytes else None,
            static_target_ids=static_target_ids,
        )
        return base_mod_id, compiled


# =============================================================================
# 4. PURE WORD INDEX EXTRACTION RULE CLASS
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

    def bundle(self):
        _get_module = self.local.device_id.get_module
        _resolve_module = self.shared.id_registry.resolve_module
        base_mod_id = _resolve_module(self.module_name).id

        suffix = getattr(self, "module_suffix", "").strip()
        static_target_id = None
        if suffix:
            try:
                static_target_id = _get_module(suffix).id
            except Exception:
                pass

        compiled = SimpleNamespace(
            mode="word",
            z_start=getattr(self, "word_index", 0),
            y_count=getattr(self, "word_count", 1),
            static_target_id=static_target_id,
        )
        return base_mod_id, compiled


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

        local_ctx = SimpleNamespace(device_id=self.local.device_id)

        # Build each rule polymorphically based on its specified structural type mapping
        for rule_cfg in config.get("rules", []):
            rule_obj = factory_build("key_value_rule", rule_cfg, system_ctx=self.shared, local_ctx=local_ctx)
            self._instantiated_rules.append(rule_obj)

        return changed

    def run(self):
        # --- Localize Framework Plumbing ---
        _time_ns = self.shared.time_ns
        _len = len
        _get = self.input_queue.get
        _distribute = self.distribute
        _module_from_int = self.shared.id_registry.module_from_int

        max_batch = self.max_batch
        max_timeout = self.delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)

        device_identity: DeviceIdentity = self.local.device_id
        device_identity_id = device_identity.id
        system_identity_id = self.shared.id_registry.get_device("SYSTEM").id
        _get_module = device_identity.get_module

        logger = self.logger

        # --- Dynamic Rule Map Compiling ---
        rules_by_module = {}
        for rule in self._instantiated_rules:
            # Zero-Overhead Filtering: Skip disabled rules during initialization
            if not getattr(rule, "enabled", True):
                logger.info(f"Skipping disabled rule for module: '{getattr(rule, 'module_name', 'unknown')}'")
                continue

            base_mod_id, compiled_ns = rule.bundle()
            logger.debug(f"rule for '{base_mod_id}' compiled: {compiled_ns}")

            # Group into arrays to prevent overlapping rules from clobbering each other
            if base_mod_id not in rules_by_module:
                rules_by_module[base_mod_id] = []
            rules_by_module[base_mod_id].append(compiled_ns)

        # --- Setup Output Batch Pools ---
        pool_create = self.shared.array_pool.create

        def batch_acquire():
            return pool_create(
                PooledLogBatch, max_batch, max_batch * 128, has_levels=True, has_modules=True, has_devices=True
            )

        parsed_batch = batch_acquire()
        last_flush_ns = _time_ns()
        module_cache = {}
        _SPACE_DELIM = b" "

        def flush():
            nonlocal parsed_batch, last_flush_ns
            if parsed_batch and parsed_batch.size > 0:
                with parsed_batch:
                    _distribute(parsed_batch)
                parsed_batch = batch_acquire()
                last_flush_ns = _time_ns()

        stop_is_set = self._stop_event.is_set

        # --- Hot Processing Loop ---
        while not stop_is_set():
            now_ns = _time_ns()

            if parsed_batch.size > 0:
                elapsed_ns = now_ns - last_flush_ns
                current_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
            else:
                current_timeout = 120.0

            batch = _get(timeout=current_timeout)

            if not batch:
                if parsed_batch.size > 0:
                    flush()
                continue

            with batch:
                for ts_ns, msg_view, level, module_id, device_id, *_ in batch:
                    if device_id == device_identity_id or device_id == system_identity_id:
                        continue

                    # Retrieve the sequence list of extraction rules linked to this module
                    rules = rules_by_module.get(module_id)
                    if rules is None:
                        continue

                    msg_bytes = None
                    cached_words = None
                    cached_total = -1

                    # Execute all rules registered under this module sequential cascade
                    for rule in rules:
                        # =================================================================
                        # DELIMITER EXTRACTION
                        # =================================================================
                        match rule.mode:
                            case "delimiter":
                                if rule.kv_delim_int is not None and rule.kv_delim_int not in msg_view:
                                    continue
                                if rule.field_delim_int is not None and rule.field_delim_int not in msg_view:
                                    continue

                                msg_bytes = msg_view.tobytes()

                                if rule.prefix_bytes and msg_bytes.startswith(rule.prefix_bytes):
                                    msg_bytes = msg_bytes[_len(rule.prefix_bytes) :]  # Low overhead byte slice

                                for chunk in msg_bytes.split(rule.field_delim):
                                    if rule.kv_delim in chunk:
                                        if _len(parts := chunk.split(rule.kv_delim, 1)) == 2:
                                            k_bytes, v_bytes = parts[0].strip(), parts[1].strip()

                                            if k_bytes and v_bytes:
                                                try:
                                                    cache_key = (module_id, k_bytes)
                                                    if cache_key not in module_cache:
                                                        parent_mod = _module_from_int(module_id)
                                                        key_str = k_bytes.decode("ascii", errors="ignore")

                                                        if rule.module_suffix:
                                                            sub_mod_name = (
                                                                f"{parent_mod.name}.{key_str}.{rule.module_suffix}"
                                                            )
                                                        else:
                                                            sub_mod_name = f"{parent_mod.name}.{key_str}"

                                                        module_cache[cache_key] = _get_module(sub_mod_name).id

                                                    target_mod_id = module_cache[cache_key]

                                                    if (
                                                        parsed_batch.size >= parsed_batch.capacity
                                                        or parsed_batch.msg_cursor + _len(v_bytes)
                                                        > parsed_batch.buffer_capacity()
                                                    ):
                                                        flush()

                                                    parsed_batch.insert(
                                                        ts_ns=ts_ns,
                                                        rx_ts_ns=ts_ns,
                                                        msg_bytes=v_bytes,
                                                        level=level if level is not None else 0,
                                                        module=target_mod_id,
                                                        device=device_identity_id,
                                                    )
                                                except Exception:
                                                    pass

                            # =================================================================
                            # TOKEN EXTRACTION
                            # =================================================================
                            case "token":
                                pat_len = _len(rule.pattern_bytes)

                                # Fast size reject: if the log line is shorter than the anchor pattern, drop it
                                if _len(msg_view) < pat_len:
                                    continue

                                # 1. Zero-Copy Boundary Filtering
                                if rule.match_type == "starts_with":
                                    # Check first byte scalar, then evaluate only the exact prefix slice
                                    if (
                                        msg_view[0] != rule.pattern_bytes[0]
                                        or msg_view[:pat_len].tobytes() != rule.pattern_bytes
                                    ):
                                        continue

                                elif rule.match_type == "ends_with":
                                    # Check last byte scalar, then evaluate only the exact suffix slice
                                    if (
                                        msg_view[-1] != rule.pattern_bytes[-1]
                                        or msg_view[-pat_len:].tobytes() != rule.pattern_bytes
                                    ):
                                        continue

                                elif rule.match_type == "contains":
                                    # Fast reject: if the first byte of the pattern isn't even in the array, drop it
                                    if rule.pattern_bytes[0] not in msg_view:
                                        continue

                                    # Fall back to a full sequence check only if the first byte hit
                                    msg_bytes = msg_view.tobytes()
                                    if rule.pattern_bytes not in msg_bytes:
                                        continue

                                if rule.match_type != "contains":
                                    msg_bytes = msg_view.tobytes()

                                words = msg_bytes.split()
                                total_words = _len(words)

                                if total_words > rule.z_start:
                                    end_idx = (
                                        total_words
                                        if rule.y_count == 0
                                        else min(rule.z_start + rule.y_count, total_words)
                                    )
                                    v_bytes = _SPACE_DELIM.join(words[rule.z_start : end_idx])

                                    if v_bytes:
                                        try:
                                            if rule.static_target_id is not None:
                                                target_mod_id = rule.static_target_id
                                            else:
                                                cache_key = (module_id, rule.pattern_bytes, rule.z_start, rule.y_count)
                                                if cache_key not in module_cache:
                                                    parent_mod = _module_from_int(module_id)
                                                    pattern_str = rule.pattern_bytes.decode("ascii", errors="ignore")
                                                    sub_mod_name = f"{parent_mod.name}.{pattern_str}_z{rule.z_start}"
                                                    module_cache[cache_key] = _get_module(sub_mod_name).id

                                                target_mod_id = module_cache[cache_key]

                                            if (
                                                parsed_batch.size >= parsed_batch.capacity
                                                or parsed_batch.msg_cursor + _len(v_bytes)
                                                > parsed_batch.buffer_capacity()
                                            ):
                                                flush()

                                            parsed_batch.insert(
                                                ts_ns=ts_ns,
                                                rx_ts_ns=ts_ns,
                                                msg_bytes=v_bytes,
                                                level=level if level is not None else 0,
                                                module=target_mod_id,
                                                device=device_identity_id,
                                            )
                                        except Exception:
                                            pass

                            # =================================================================
                            # JSON-LITE EXTRACTION
                            # =================================================================
                            case "json_lite":
                                if not rule.json_key_bytes or rule.json_key_bytes not in msg_view:
                                    continue

                                msg_bytes = msg_view.tobytes()
                                idx = msg_bytes.find(rule.json_key_bytes)
                                if idx == -1:
                                    continue

                                pos = idx + _len(rule.json_key_bytes)
                                msg_len = _len(msg_bytes)

                                while pos < msg_len and msg_bytes[pos] == 32:  # ASCII 32 = Space
                                    pos += 1

                                if pos >= msg_len:
                                    continue

                                v_bytes = b""
                                if msg_bytes[pos] == 34:  # ASCII 34 = '"'
                                    pos += 1
                                    end_pos = msg_bytes.find(b'"', pos)
                                    if end_pos != -1:
                                        v_bytes = msg_bytes[pos:end_pos]
                                else:
                                    end_pos = pos
                                    while end_pos < msg_len and msg_bytes[end_pos] not in (44, 125, 93, 32, 10, 13):
                                        end_pos += 1
                                    v_bytes = msg_bytes[pos:end_pos]

                                if v_bytes:
                                    try:
                                        if rule.static_target_id is not None:
                                            target_mod_id = rule.static_target_id
                                        else:
                                            cache_key = (module_id, rule.json_key_bytes)
                                            if cache_key not in module_cache:
                                                parent_mod = _module_from_int(module_id)
                                                key_str = (
                                                    rule.json_key_bytes[:-2]
                                                    .decode("ascii", errors="ignore")
                                                    .replace('"', "")
                                                )
                                                module_cache[cache_key] = _get_module(f"{parent_mod.name}.{key_str}").id
                                            target_mod_id = module_cache[cache_key]

                                        if (
                                            parsed_batch.size >= parsed_batch.capacity
                                            or parsed_batch.msg_cursor + _len(v_bytes) > parsed_batch.buffer_capacity()
                                        ):
                                            flush()

                                        parsed_batch.insert(
                                            ts_ns=ts_ns,
                                            rx_ts_ns=ts_ns,
                                            msg_bytes=v_bytes,
                                            level=level if level is not None else 0,
                                            module=target_mod_id,
                                            device=device_identity_id,
                                        )
                                    except Exception:
                                        pass
                            # =================================================================
                            # LIST DELIMITER (POSITIONAL STRUCT) EXTRACTION
                            # =================================================================
                            case "list_delimiter":
                                # 1. High-Speed Optional Signature Guard
                                if rule.start_bytes:
                                    start_len = _len(rule.start_bytes)
                                    if _len(msg_view) < start_len:
                                        continue
                                    # Fast scalar short-circuit + exact sub-slice string matching
                                    if (
                                        msg_view[0] != rule.start_bytes[0]
                                        or msg_view[:start_len].tobytes() != rule.start_bytes
                                    ):
                                        continue

                                # 2. Fast delimiter component verification
                                if rule.field_delim_int is not None and rule.field_delim_int not in msg_view:
                                    continue

                                msg_bytes = msg_view.tobytes()

                                if rule.prefix_bytes and msg_bytes.startswith(rule.prefix_bytes):
                                    msg_bytes = msg_bytes[_len(rule.prefix_bytes) :]

                                # 3. Position index parameter loop
                                for i, chunk in enumerate(msg_bytes.split(rule.field_delim)):
                                    if i >= _len(rule.static_target_ids):
                                        break

                                    target_mod_id = rule.static_target_ids[i]
                                    if target_mod_id is None:
                                        continue

                                    v_bytes = chunk.strip()
                                    if not v_bytes:
                                        continue

                                    if (
                                        parsed_batch.size >= parsed_batch.capacity
                                        or parsed_batch.msg_cursor + _len(v_bytes) > parsed_batch.buffer_capacity()
                                    ):
                                        flush()

                                    parsed_batch.insert(
                                        ts_ns=ts_ns,
                                        rx_ts_ns=ts_ns,
                                        msg_bytes=v_bytes,
                                        level=level if level is not None else 0,
                                        module=target_mod_id,
                                        device=device_identity_id,
                                    )

                            # =================================================================
                            # PURE WORD INDEX SLICE EXTRACTION
                            # =================================================================
                            case "word":
                                # Lazy cache split across all consecutive word rules for this message
                                if cached_words is None:
                                    if msg_bytes is None:
                                        msg_bytes = msg_view.tobytes()
                                    cached_words = msg_bytes.split()
                                    cached_total = _len(cached_words)

                                if cached_total > rule.z_start:
                                    end_idx = (
                                        cached_total
                                        if rule.y_count == 0
                                        else min(rule.z_start + rule.y_count, cached_total)
                                    )
                                    v_bytes = _SPACE_DELIM.join(cached_words[rule.z_start : end_idx])

                                    if v_bytes:
                                        try:
                                            if (
                                                parsed_batch.size >= parsed_batch.capacity
                                                or parsed_batch.msg_cursor + _len(v_bytes)
                                                > parsed_batch.buffer_capacity()
                                            ):
                                                flush()

                                            parsed_batch.insert(
                                                ts_ns=ts_ns,
                                                rx_ts_ns=ts_ns,
                                                msg_bytes=v_bytes,
                                                level=level if level is not None else 0,
                                                module=rule.static_target_id
                                                if rule.static_target_id is not None
                                                else module_id,
                                                device=device_identity_id,
                                            )
                                        except Exception:
                                            pass

                    if parsed_batch.size >= max_batch:
                        flush()

            if parsed_batch.size > 0 and (_time_ns() - last_flush_ns >= max_timeout_ns):
                flush()

        if parsed_batch:
            if parsed_batch.size > 0:
                flush()
            else:
                parsed_batch.release()
