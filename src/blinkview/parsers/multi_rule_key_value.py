# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace
from typing import TYPE_CHECKING

import numpy as np
from numba import typeof, types
from numba.typed import List as NumbaList

from blinkview.core import dtypes
from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable, configuration_property, override_property
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.system_context import SystemContext
from blinkview.core.types.empty import EMPTY_BYTES_RO, EMPTY_ID
from blinkview.core.types.kv_extraction import EmptyKvRuleConfig, KvExtractState, KvRuleConfig, KvRuleID
from blinkview.core.types.modules import MODULE_ID_UNKNOWN, MODULE_TEMP_ID_BASE, ModuleTrackerState
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.kv_extraction import nb_process_kv_batch
from blinkview.parsers.parser import BaseParser, ParserFactory
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

# One shared scratch tracker per parser instance, sized generously enough that a realistic burst
# of genuinely-new key names within one output-batch cycle never exhausts it (mirrors
# ModuleNameParserBase's TRACKER_CAPACITY/AVG_NAME_LEN constants in parsers/frame_parsers.py -
# AVG_NAME_LEN is a little larger here since a KEY_VALUE rule's candidate string is
# "parent.key", not just a bare tag).
KV_TRACKER_CAPACITY = 1024
KV_TRACKER_AVG_NAME_LEN = 96

_kv_config_type = typeof(EmptyKvRuleConfig)
_kv_rule_tuple_type = types.Tuple((types.int64, types.int64, _kv_config_type))


def _ascii_bytes_ro(s: str) -> np.ndarray:
    """Read-only uint8 view over an ASCII-encoded string - EMPTY_BYTES_RO is also read-only
    (np.frombuffer(b"", ...)), so every KvRuleConfig byte-array field is consistently read-only
    regardless of whether a given rule actually uses it - required for a numba.typed.List of a
    single homogeneous tuple type (Numba types read-only-ness as part of an array's type)."""
    return np.frombuffer(s.encode("ascii"), dtype=dtypes.BYTE)


@configurable
@bindable
@configuration_property("enabled", type="boolean", default=True, ui_order=2, title="Enabled")
class ExtractionRule:
    """Base class for section-based polymorphic log extraction rules. bundle() returns a static
    (base_module_id, KvRuleID, KvRuleConfig) triple - all per-row dynamic work happens inside the
    Numba kernels in ops/kv_extraction.py, not here."""

    shared: SystemContext
    local: SimpleNamespace

    module_name: str
    module_suffix: str

    def bundle(self) -> tuple[int, int, KvRuleConfig]:
        raise NotImplementedError


@register_factory_category(FactoryCategory.KEY_VALUE_RULE)
class ExtractionRuleFactory(BaseFactory[ExtractionRule]):
    pass


@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", default="", ui_order=10, title="Module Name Suffix")
@configuration_property(
    "prefix_strip", type="string", default="", ui_order=12, title="Prefix to Strip (e.g., 'Data: ')"
)
@configuration_property("field_delimiter", type="string", default=" ", ui_order=15, title="Field Delimiter")
@configuration_property("kv_delimiter", type="string", default="=", ui_order=20, title="Key-Value Delimiter")
@ExtractionRuleFactory.register("key_value")
class KeyValueExtractionRule(ExtractionRule):
    """Extracts key=value pairs with a delimiter. The only rule type needing a per-row dynamic
    module lookup - the key text (and therefore the target submodule) varies per row, so it's
    resolved at kernel time via the shared discovery tracker (core/types/kv_extraction.py)."""

    prefix_strip: str
    field_delimiter: str
    kv_delimiter: str

    def bundle(self) -> tuple[int, int, KvRuleConfig]:
        resolve_module = self.shared.id_registry.resolve_module

        parent_mod = resolve_module(self.module_name)
        base_mod_id = parent_mod.id

        suffix = self.module_suffix.strip() or None
        name_prefix = f"{suffix}." if suffix else f"{parent_mod.name}."

        field_delim = self.field_delimiter.encode("ascii")
        kv_delim = self.kv_delimiter.encode("ascii")

        field_delim_int = field_delim[0] if len(field_delim) == 1 else 32
        kv_delim_int = kv_delim[0] if len(kv_delim) == 1 else 61

        cfg = KvRuleConfig(
            field_delim=field_delim_int,
            kv_delim=kv_delim_int,
            prefix_bytes=_ascii_bytes_ro(self.prefix_strip) if self.prefix_strip else EMPTY_BYTES_RO,
            name_prefix_bytes=_ascii_bytes_ro(name_prefix.lower()),
        )

        return base_mod_id, KvRuleID.KEY_VALUE, cfg


@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", required=True, default="", ui_order=10, title="New module name")
@configuration_property(
    "match",
    type="string",
    enum=["starts_with", "contains", "ends_with"],
    required=True,
    default="contains",
    ui_order=15,
)
@configuration_property("pattern", type="string", required=True, default="", ui_order=20, title="Pattern")
@configuration_property("index", type="integer", required=True, default=1, ui_order=25, title="Start Index")
@configuration_property("count", type="integer", required=False, default=1, ui_order=30, title="Word Count (0 = All)")
@ExtractionRuleFactory.register("anchor_word")
class AnchorWordExtractionRule(ExtractionRule):
    """Extract values matching startswith/endswith/contains patterns."""

    match: str
    pattern: str
    index: int
    count: int

    def bundle(self) -> tuple[int, int, KvRuleConfig]:
        resolve_module = self.shared.id_registry.resolve_module
        get_module = self.local.device_id.get_module

        parent_mod = resolve_module(self.module_name)
        base_mod_id = parent_mod.id

        match_mode = {"starts_with": 0, "ends_with": 1, "contains": 2}.get(self.match, 2)

        suffix = self.module_suffix.strip() or None
        static_target_id = MODULE_ID_UNKNOWN
        if suffix:
            try:
                static_target_id = get_module(suffix).id
            except Exception:
                pass
        else:
            # No suffix configured - the target is a fixed, rule-derived name. z_start/pattern
            # are both static per rule instance, so (unlike the original pure-Python
            # implementation, which resolved this lazily on first row) this can be resolved once,
            # eagerly, right here.
            try:
                pattern_str = self.pattern
                sub_mod_name = f"{parent_mod.name}.{pattern_str}_z{self.index}"
                static_target_id = get_module(sub_mod_name).id
            except Exception:
                pass

        cfg = KvRuleConfig(
            pattern_bytes=_ascii_bytes_ro(self.pattern),
            match_mode=match_mode,
            word_index=self.index,
            word_count=self.count,
            static_target_id=static_target_id,
        )

        return base_mod_id, KvRuleID.ANCHOR_WORD, cfg


@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", default="", ui_order=10, title="New Module Name")
@configuration_property("json_key", type="string", default="", required=True, ui_order=15, title="JSON Key to Extract")
class JsonLiteExtractionRule(ExtractionRule):
    json_key: str

    def bundle(self) -> tuple[int, int, KvRuleConfig]:
        get_module = self.local.device_id.get_module

        parent_mod = get_module(self.module_name)
        base_mod_id = parent_mod.id

        suffix = getattr(self, "module_suffix", "").strip() or None
        static_target_id = MODULE_ID_UNKNOWN
        if suffix:
            try:
                static_target_id = get_module(f"{self.module_name}.{suffix}").id
            except Exception:
                pass
        else:
            json_key_raw = getattr(self, "json_key", "").strip()
            if json_key_raw:
                try:
                    static_target_id = get_module(f"{parent_mod.name}.{json_key_raw}").id
                except Exception:
                    pass

        json_key_raw = getattr(self, "json_key", "").strip()
        json_key_bytes = f'"{json_key_raw}":' if json_key_raw else ""

        cfg = KvRuleConfig(
            pattern_bytes=_ascii_bytes_ro(json_key_bytes) if json_key_bytes else EMPTY_BYTES_RO,
            static_target_id=static_target_id,
        )

        return base_mod_id, KvRuleID.JSON_LITE, cfg


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
@ExtractionRuleFactory.register("dsv")
class DsvExtractionRule(ExtractionRule):
    """Extract from CSV-like formats using object-mapped positional schema rules."""

    startswith: str
    prefix_strip: str
    field_delimiter: str
    field_names: list

    def bundle(self) -> tuple[int, int, KvRuleConfig]:
        resolve_module = self.shared.id_registry.resolve_module
        get_module = self.local.device_id.get_module

        parent_mod = resolve_module(self.module_name)
        base_mod_id = parent_mod.id

        suffix = getattr(self, "module_suffix", "").strip()
        if suffix:
            try:
                parent_mod = get_module(suffix)
            except Exception:
                pass

        field_delim = getattr(self, "field_delimiter", ";").encode("ascii")
        field_delim_int = field_delim[0] if len(field_delim) == 1 else 59

        field_target_ids = []
        for field_cfg in getattr(self, "field_names", []):
            if not field_cfg:
                field_target_ids.append(MODULE_ID_UNKNOWN)
                continue
            get_fval = (
                (lambda k, d: field_cfg.get(k, d))
                if isinstance(field_cfg, dict)
                else (lambda k, d: getattr(field_cfg, k, d))
            )
            ignore = get_fval("ignore", False)
            name = get_fval("name", "").strip()
            if ignore or not name:
                field_target_ids.append(MODULE_ID_UNKNOWN)
                continue
            try:
                field_target_ids.append(get_module(f"{parent_mod.name}.{name}").id)
            except Exception:
                field_target_ids.append(MODULE_ID_UNKNOWN)

        cfg = KvRuleConfig(
            field_delim=field_delim_int,
            prefix_bytes=_ascii_bytes_ro(self.prefix_strip) if getattr(self, "prefix_strip", "") else EMPTY_BYTES_RO,
            pattern_bytes=_ascii_bytes_ro(self.startswith.strip())
            if getattr(self, "startswith", "").strip()
            else EMPTY_BYTES_RO,
            field_target_ids=np.array(field_target_ids, dtype=dtypes.ID_TYPE) if field_target_ids else EMPTY_ID,
        )

        return base_mod_id, KvRuleID.DSV, cfg


@configuration_property("module_name", type="string", required=True, ui_order=5, title="Match module")
@configuration_property("module_suffix", type="string", required=True, ui_order=10, title="New Module Name")
@configuration_property("word_index", type="integer", default=0, ui_order=15, title="Word Index (Z)")
@configuration_property("word_count", type="integer", default=1, ui_order=20, title="Word Count (0 = All remaining)")
@ExtractionRuleFactory.register("positional")
class PositionalExtractionRule(ExtractionRule):
    """Extracts a specific word or slice of words directly by its whitespace-split index."""

    word_index: int
    word_count: int

    def bundle(self) -> tuple[int, int, KvRuleConfig]:
        resolve_module = self.shared.id_registry.resolve_module
        get_module = self.local.device_id.get_module

        base_mod_id = resolve_module(self.module_name).id
        static_target_id = get_module(self.module_suffix.strip()).id

        cfg = KvRuleConfig(
            word_index=getattr(self, "word_index", 0),
            word_count=getattr(self, "word_count", 1),
            static_target_id=static_target_id,
        )

        return base_mod_id, KvRuleID.POSITIONAL, cfg


@configuration_property(
    "rules",
    type="array",
    required=True,
    ui_order=10,
    title="Polymorphic Parsing Rules",
    items={
        "type": "object",
        "_factory": FactoryCategory.KEY_VALUE_RULE,
        "title": "Extraction Rule Setup",
    },
)
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/targets"},
)
@ParserFactory.register("multi_rule_key_value")
class MultiRuleKeyValueParser(BaseParser):
    __doc__ = """Extracts structured key=value/JSON/DSV/positional/anchor-word fields out of raw
log rows into synthetic submodules, via a single Numba-JIT kernel call per input-batch chunk
(ops/kv_extraction.py's nb_process_kv_batch) - see plans/kv-extractor-numba-backend.md."""

    rules: list

    def __init__(self):
        super().__init__()
        self._instantiated_rules = []
        self._tracker_state = None
        self._extract_state = None
        self._tracker_starts_mv = None
        self._tracker_lengths_mv = None
        self._tracker_name_bytes_mv = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        self._instantiated_rules = []
        factory_build = self.shared.factories.build
        local_ctx = SimpleNamespace(device_id=self.local.device_id)

        for rule_cfg in config.get("rules", []):
            rule_obj = factory_build(
                FactoryCategory.KEY_VALUE_RULE, rule_cfg, system_ctx=self.shared, local_ctx=local_ctx
            )
            self._instantiated_rules.append(rule_obj)

        if self._tracker_state is None:
            self._tracker_state = ModuleTrackerState(
                count=np.zeros(1, dtypes.ID_TYPE),
                bytes_cursor=np.zeros(1, dtypes.OFFSET_TYPE),
                starts=np.empty(KV_TRACKER_CAPACITY, dtypes.OFFSET_TYPE),
                lengths=np.empty(KV_TRACKER_CAPACITY, dtypes.LEN_TYPE),
                hashes=np.zeros(KV_TRACKER_CAPACITY, dtypes.HASH_TYPE),
                name_bytes=np.empty(KV_TRACKER_CAPACITY * KV_TRACKER_AVG_NAME_LEN, dtype=dtypes.BYTE),
            )
            # Buffers above are fixed-capacity and never reallocated for the parser's lifetime,
            # so the memoryviews (cheaper per-element access than numpy scalar indexing) can be
            # built once here instead of on every _post_process() call.
            self._tracker_starts_mv = memoryview(self._tracker_state.starts)
            self._tracker_lengths_mv = memoryview(self._tracker_state.lengths)
            self._tracker_name_bytes_mv = memoryview(self._tracker_state.name_bytes)

        if self._extract_state is None:
            self._extract_state = KvExtractState(in_idx=np.zeros(1, dtype=np.int64))

        return changed

    def _build_rules_list(self):
        rules = NumbaList.empty_list(_kv_rule_tuple_type)
        for rule in self._instantiated_rules:
            if not getattr(rule, "enabled", True):
                self.logger.info("Skipping disabled rule: '%s'", getattr(rule, "module_name", "unknown"))
                continue

            base_mod_id, rule_id, cfg = rule.bundle()
            self.logger.debug("Compiled rule for module_id=%s: %s", base_mod_id, rule.__class__.__name__)
            rules.append((base_mod_id, rule_id, cfg))

        return rules

    def _post_process(self, batch_out: PooledLogBatch) -> bool:
        """Resolves any KEY_VALUE-discovered temp module ids in batch_out into real registry
        ids, via one real get_module() call per *distinct new name*, then a vectorized swap
        across the whole batch - mirrors ModuleNameParserBase.post_process (parsers/
        frame_parsers.py) exactly, just against the flat tracker this parser owns directly
        rather than a UnifiedParserState-wrapped one."""
        state = self._tracker_state
        unresolved_count = state.count[0]
        if unresolved_count == 0:
            return False

        registry = self.shared.id_registry.modules_table
        initial_count = registry.count

        active_modules = batch_out.bundle.modules[: batch_out.size]
        get_module = self.local.device_id.get_module
        starts = self._tracker_starts_mv
        lengths = self._tracker_lengths_mv
        name_bytes = self._tracker_name_bytes_mv

        for i in range(unresolved_count):
            start = starts[i]
            length = lengths[i]

            module_name_str = name_bytes[start : start + length].tobytes().decode("ascii")

            try:
                mod_id = get_module(module_name_str).id
            except Exception:
                self.logger.exception("Failed to register discovered KV module '%s'", module_name_str)
                mod_id = get_module("unknown").id

            temp_id = MODULE_TEMP_ID_BASE + i
            active_modules[active_modules == temp_id] = mod_id

        state.count[0] = 0
        state.bytes_cursor[0] = 0

        return registry.count > initial_count

    def run(self):
        _time_ns = self.shared.time_ns
        _get = self.input_queue.get
        _distribute = self.distribute

        max_timeout = self.delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)

        device_identity = self.local.device_id
        device_identity_id = device_identity.id
        system_identity_id = self.shared.id_registry.get_device("SYSTEM").id

        logger = self.logger

        rules = self._build_rules_list()
        tracker = self._tracker_state
        extract_state = self._extract_state

        def current_string_table():
            # Refetched every call (cheap: IndexedStringTable.bundle() memoizes and only rebuilds
            # on real growth) rather than cached once, so a module registered by _post_process
            # earlier in this same run() loop is visible to the very next kernel call.
            return device_identity.modules_table.bundle()

        speed_out = Speedometer(logger=self.logger.child("stats_out"))
        tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

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

        batch_out = batch_acquire()
        batch_out_time = _time_ns()

        def flush():
            nonlocal batch_out, batch_out_time
            if batch_out and batch_out.size > 0:
                with batch_out:
                    tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=max_timeout)
                    _distribute(batch_out)
            batch_out = batch_acquire()
            batch_out_time = _time_ns()

        stop_is_set = self._stop_event.is_set

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
                    if batch_in.bundle is None or batch_in.size == 0:
                        continue

                    if batch_out.size == 0:
                        batch_out_time = _time_ns()

                    extract_state.in_idx[0] = 0

                    while True:
                        out_is_full = nb_process_kv_batch(
                            batch_in.bundle,
                            extract_state,
                            rules,
                            tracker,
                            current_string_table(),
                            batch_out.bundle,
                            device_identity_id,
                            system_identity_id,
                            device_identity_id,
                        )

                        if self._post_process(batch_out):
                            # A genuinely new module was registered - rebuild the rule list isn't
                            # necessary (rule bundles don't reference the string table directly),
                            # but current_string_table() will pick up the growth on its own next
                            # call since it re-fetches the bundle every time.
                            pass

                        if out_is_full:
                            flush()
                        else:
                            break

                except Exception as e:
                    logger.exception("Poison batch encountered, skipping remainder.", exc=e)

            if batch_out.size > 0 and (_time_ns() - batch_out_time >= max_timeout_ns):
                flush()

        if batch_out:
            if batch_out.size > 0:
                flush()
            else:
                batch_out.release()

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for every KvRuleID branch (including the KEY_VALUE dynamic
        tracker/resolve path) via one real nb_process_kv_batch call - the single-kernel-call
        entry point every real MultiRuleKeyValueParser.run() iteration actually uses."""
        print("[Warmup] MultiRuleKeyValueParser ...")

        device = helper.warmup_mod.device
        local_ctx = SimpleNamespace(device_id=device)

        def build(rule_cls, **overrides):
            rule = rule_cls()
            rule.shared = helper.shared
            rule.local = local_ctx
            hydrated = rule.hydrate_config(overrides)
            rule.apply_config(hydrated)
            return rule.bundle()

        rules_raw = [
            build(KeyValueExtractionRule, module_name="numba.warmup"),
            build(
                AnchorWordExtractionRule,
                module_name="numba.warmup",
                module_suffix="numba.warmup.anchor",
                match="contains",
                pattern="TOK",
                index=0,
                count=1,
            ),
            build(JsonLiteExtractionRule, module_name="numba.warmup", json_key="k"),
            build(
                DsvExtractionRule,
                module_name="numba.warmup",
                field_delimiter=";",
                field_names=[{"name": "f1"}],
            ),
            build(
                PositionalExtractionRule,
                module_name="numba.warmup",
                module_suffix="numba.warmup.pos",
                word_index=0,
                word_count=1,
            ),
        ]

        kv_config_type = typeof(EmptyKvRuleConfig)
        kv_rule_type = types.Tuple((types.int64, types.int64, kv_config_type))
        rules = NumbaList.empty_list(kv_rule_type)
        for mod_id, rule_id, cfg in rules_raw:
            rules.append((mod_id, rule_id, cfg))

        tracker = ModuleTrackerState(
            count=np.zeros(1, dtypes.ID_TYPE),
            bytes_cursor=np.zeros(1, dtypes.OFFSET_TYPE),
            starts=np.empty(KV_TRACKER_CAPACITY, dtypes.OFFSET_TYPE),
            lengths=np.empty(KV_TRACKER_CAPACITY, dtypes.LEN_TYPE),
            hashes=np.zeros(KV_TRACKER_CAPACITY, dtypes.HASH_TYPE),
            name_bytes=np.empty(KV_TRACKER_CAPACITY * KV_TRACKER_AVG_NAME_LEN, dtype=dtypes.BYTE),
        )

        # Dummy rows are tagged with a *different* device (helper.floats_mod's) than the parser's
        # own (device.id) - nb_process_kv_batch skips rows tagged with the parser's own device
        # (or SYSTEM) exactly like the real run() loop does, so using the same device here would
        # skip every warmup row and never actually exercise the rule kernels.
        source_device_id = helper.floats_mod.device.id

        with (
            helper.array_pool.create(
                PooledLogBatch, 8, 512, has_levels=True, has_modules=True, has_devices=True
            ) as dummy_in,
            helper.array_pool.create(
                PooledLogBatch, 8, 512, has_levels=True, has_modules=True, has_devices=True
            ) as dummy_out,
        ):
            time_ns = helper.time_ns
            module_id = helper.warmup_mod.id
            dummy_in.insert(time_ns(), time_ns(), b"a=1", module=module_id, device=source_device_id)
            dummy_in.insert(time_ns(), time_ns(), b"one TOK two", module=module_id, device=source_device_id)
            dummy_in.insert(time_ns(), time_ns(), b'{"k":"v"}', module=module_id, device=source_device_id)
            dummy_in.insert(time_ns(), time_ns(), b"A;B", module=module_id, device=source_device_id)
            dummy_in.insert(time_ns(), time_ns(), b"one two", module=module_id, device=source_device_id)

            state = KvExtractState(in_idx=np.zeros(1, dtype=np.int64))

            nb_process_kv_batch(
                dummy_in.bundle,
                state,
                rules,
                tracker,
                device.modules_table.bundle(),
                dummy_out.bundle,
                device.id,
                helper.registry.get_device("SYSTEM").id,
                device.id,
            )

        print("[Warmup] MultiRuleKeyValueParser ... done")
