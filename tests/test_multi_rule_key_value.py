# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.kv_extraction import KvExtractState, KvRuleID
from blinkview.core.types.modules import MODULE_ID_UNKNOWN, ModuleTrackerState
from blinkview.ops.kv_extraction import (
    nb_extract_anchor_word_row,
    nb_extract_dsv_row,
    nb_extract_json_lite_row,
    nb_extract_key_value_row,
    nb_extract_positional_row,
    nb_process_kv_batch,
)
from blinkview.parsers.multi_rule_key_value import (
    AnchorWordExtractionRule,
    DsvExtractionRule,
    ExtractionRuleFactory,
    JsonLiteExtractionRule,
    KeyValueExtractionRule,
    MultiRuleKeyValueParser,
    PositionalExtractionRule,
)


def configure(instance, **overrides):
    """Mirrors BaseFactory.build(): hydrate the schema defaults into the config, then apply."""
    hydrated = instance.hydrate_config(overrides)
    instance.apply_config(hydrated)
    return instance


def _make_rule(rule_cls, id_registry, device_name, **overrides):
    rule = configure(rule_cls(), **overrides)
    device = id_registry.get_device(device_name)
    rule.shared = SimpleNamespace(id_registry=id_registry)
    rule.local = SimpleNamespace(device_id=device)
    return rule, device


def _in_batch(pool, message: str, level=0):
    batch = pool.create(PooledLogBatch, 4, 512, has_levels=True, has_modules=True, has_devices=True)
    batch.insert(1000, 1000, message.encode("ascii"), level=level)
    return batch


def _out_batch(pool, capacity=8, buffer_bytes=512):
    return pool.create(PooledLogBatch, capacity, buffer_bytes, has_levels=True, has_modules=True, has_devices=True)


def _rows(batch):
    return [(bytes(msg), int(module)) for _ts, msg, _rx, _level, module, *_rest in batch]


def resolve_temp_ids(tracker, device, out_batch):
    """Mirrors MultiRuleKeyValueParser._post_process for tests that call the KEY_VALUE kernel
    directly (bypassing the real parser's run() loop): resolves any pending temp module ids into
    real registry ids and swaps them into out_batch's modules column."""
    from blinkview.core.types.modules import MODULE_TEMP_ID_BASE

    unresolved_count = tracker.count[0]
    if unresolved_count == 0:
        return

    active_modules = out_batch.bundle.modules[: out_batch.size]
    starts = memoryview(tracker.starts)
    lengths = memoryview(tracker.lengths)
    name_bytes = memoryview(tracker.name_bytes)
    for i in range(unresolved_count):
        start = starts[i]
        length = lengths[i]
        module_name_str = name_bytes[start : start + length].tobytes().decode("ascii")
        mod_id = device.get_module(module_name_str).id
        temp_id = MODULE_TEMP_ID_BASE + i
        active_modules[active_modules == temp_id] = mod_id

    tracker.count[0] = 0
    tracker.bytes_cursor[0] = 0


def make_kv_tracker():
    return ModuleTrackerState(
        count=np.zeros(1, dtypes.ID_TYPE),
        bytes_cursor=np.zeros(1, dtypes.OFFSET_TYPE),
        starts=np.empty(64, dtypes.OFFSET_TYPE),
        lengths=np.empty(64, dtypes.LEN_TYPE),
        hashes=np.zeros(64, dtypes.HASH_TYPE),
        name_bytes=np.empty(64 * 96, dtype=dtypes.BYTE),
    )


class TestKeyValueExtractionRule:
    def test_extracts_simple_key_value_pairs(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(KeyValueExtractionRule, id_registry, "kv_dev", module_name="kv_dev.parent")
        in_batch = _in_batch(pool, "voltage=3.3 current=1.2")
        out_batch = _out_batch(pool)
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()

        base_mod_id, rule_id, cfg = rule.bundle()
        assert rule_id == KvRuleID.KEY_VALUE

        nb_extract_key_value_row(in_batch.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)
        resolve_temp_ids(tracker, device, out_batch)

        rows = _rows(out_batch)
        voltage_mod = device.get_module("parent.voltage").id
        current_mod = device.get_module("parent.current").id
        assert (b"3.3", voltage_mod) in rows
        assert (b"1.2", current_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_strips_configured_prefix_before_parsing(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            KeyValueExtractionRule,
            id_registry,
            "kv_prefix_dev",
            module_name="kv_prefix_dev.parent",
            prefix_strip="Data: ",
        )
        in_batch = _in_batch(pool, "Data: level=high")
        out_batch = _out_batch(pool)
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_key_value_row(in_batch.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)
        resolve_temp_ids(tracker, device, out_batch)

        rows = _rows(out_batch)
        level_mod = device.get_module("parent.level").id
        assert (b"high", level_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_quoted_values_keep_internal_spaces(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            KeyValueExtractionRule, id_registry, "kv_quote_dev", module_name="kv_quote_dev.parent"
        )
        in_batch = _in_batch(pool, 'msg="hello world" done=1')
        out_batch = _out_batch(pool)
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_key_value_row(in_batch.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)
        resolve_temp_ids(tracker, device, out_batch)

        rows = _rows(out_batch)
        msg_mod = device.get_module("parent.msg").id
        assert (b"hello world", msg_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_module_suffix_overrides_the_name_prefix(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            KeyValueExtractionRule,
            id_registry,
            "kv_suffix_dev",
            module_name="kv_suffix_dev.parent",
            module_suffix="renamed",
        )
        in_batch = _in_batch(pool, "a=1")
        out_batch = _out_batch(pool)
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_key_value_row(in_batch.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)
        resolve_temp_ids(tracker, device, out_batch)

        rows = _rows(out_batch)
        expected_mod = device.get_module("renamed.a").id
        assert (b"1", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_custom_delimiters(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            KeyValueExtractionRule,
            id_registry,
            "kv_delim_dev",
            module_name="kv_delim_dev.parent",
            field_delimiter=";",
            kv_delimiter=":",
        )
        in_batch = _in_batch(pool, "a:1;b:2")
        out_batch = _out_batch(pool)
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_key_value_row(in_batch.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)
        resolve_temp_ids(tracker, device, out_batch)

        rows = _rows(out_batch)
        a_mod = device.get_module("parent.a").id
        b_mod = device.get_module("parent.b").id
        assert (b"1", a_mod) in rows
        assert (b"2", b_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_previously_seen_key_reuses_temp_id_without_growing_registry(self, id_registry):
        """Two rows with the same new key must resolve to the same module id within one tracker
        cycle - the temp-id cache-hit path in nb_resolve_module_id, not just the promote-new path."""
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            KeyValueExtractionRule, id_registry, "kv_reuse_dev", module_name="kv_reuse_dev.parent"
        )
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()

        in1 = _in_batch(pool, "reading=1")
        nb_extract_key_value_row(in1.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)
        in2 = _in_batch(pool, "reading=2")
        nb_extract_key_value_row(in2.bundle, 0, cfg, tracker, string_table, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        mods = {mod for _msg, mod in rows}
        assert len(mods) == 1  # same temp id both times, resolved to the same eventual module

        in1.release()
        in2.release()
        out_batch.release()


class TestAnchorWordExtractionRule:
    def test_contains_match_extracts_word_at_index(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            AnchorWordExtractionRule,
            id_registry,
            "anchor_dev",
            module_name="anchor_dev.parent",
            module_suffix="extracted",
            match="contains",
            pattern="TOKEN",
            index=2,
            count=1,
        )
        in_batch = _in_batch(pool, "info TOKEN abc def ghi")
        out_batch = _out_batch(pool)

        _base_mod_id, rule_id, cfg = rule.bundle()
        assert rule_id == KvRuleID.ANCHOR_WORD
        assert cfg.static_target_id != MODULE_ID_UNKNOWN

        nb_extract_anchor_word_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"abc", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_no_match_produces_no_row(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            AnchorWordExtractionRule,
            id_registry,
            "anchor_nomatch_dev",
            module_name="anchor_nomatch_dev.parent",
            module_suffix="extracted",
            match="contains",
            pattern="NOPE",
            index=0,
            count=1,
        )
        in_batch = _in_batch(pool, "info TOKEN abc")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_anchor_word_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        assert _rows(out_batch) == []

        in_batch.release()
        out_batch.release()

    def test_starts_with_match_type(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            AnchorWordExtractionRule,
            id_registry,
            "anchor_sw_dev",
            module_name="anchor_sw_dev.parent",
            module_suffix="extracted",
            match="starts_with",
            pattern="info",
            index=1,
            count=1,
        )
        in_batch = _in_batch(pool, "info TOKEN abc")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_anchor_word_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"TOKEN", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_ends_with_match_type_rejects_non_matching(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            AnchorWordExtractionRule,
            id_registry,
            "anchor_ew_dev",
            module_name="anchor_ew_dev.parent",
            module_suffix="extracted",
            match="ends_with",
            pattern="xyz",
            index=0,
            count=1,
        )
        in_batch = _in_batch(pool, "info TOKEN abc")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_anchor_word_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        assert _rows(out_batch) == []

        in_batch.release()
        out_batch.release()

    def test_count_zero_extracts_all_remaining_words(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            AnchorWordExtractionRule,
            id_registry,
            "anchor_all_dev",
            module_name="anchor_all_dev.parent",
            module_suffix="extracted",
            match="contains",
            pattern="TOKEN",
            index=1,
            count=0,
        )
        in_batch = _in_batch(pool, "info TOKEN abc def ghi")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_anchor_word_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"TOKEN abc def ghi", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_no_suffix_resolves_a_derived_static_target_at_bundle_time(self, id_registry):
        """No module_suffix configured: the target module name is derived from pattern/index and
        resolved once, eagerly, in bundle() - not per-row."""
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            AnchorWordExtractionRule,
            id_registry,
            "anchor_derived_dev",
            module_name="anchor_derived_dev.parent",
            module_suffix="",
            match="contains",
            pattern="TOKEN",
            index=1,
            count=1,
        )
        in_batch = _in_batch(pool, "info TOKEN abc")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        assert cfg.static_target_id != MODULE_ID_UNKNOWN

        nb_extract_anchor_word_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("parent.TOKEN_z1").id
        assert (b"TOKEN", expected_mod) in rows  # word_index=1 -> "info"(0) "TOKEN"(1) "abc"(2)

        in_batch.release()
        out_batch.release()


class TestJsonLiteExtractionRule:
    def test_extracts_quoted_string_value(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(JsonLiteExtractionRule, id_registry, "json_dev", module_name="parent", json_key="key")
        in_batch = _in_batch(pool, '{"key":"value","other":1}')
        out_batch = _out_batch(pool)

        _base_mod_id, rule_id, cfg = rule.bundle()
        assert rule_id == KvRuleID.JSON_LITE

        nb_extract_json_lite_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("parent.key").id
        assert (b"value", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_extracts_unquoted_numeric_value(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            JsonLiteExtractionRule, id_registry, "json_num_dev", module_name="parent", json_key="count"
        )
        in_batch = _in_batch(pool, '{"count":42,"x":1}')
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_json_lite_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("parent.count").id
        assert (b"42", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_empty_json_key_is_a_permanent_noop(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            JsonLiteExtractionRule, id_registry, "json_empty_dev", module_name="parent", json_key=""
        )
        in_batch = _in_batch(pool, '{"key":"value"}')
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_json_lite_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        assert _rows(out_batch) == []

        in_batch.release()
        out_batch.release()

    def test_key_not_found_produces_no_row(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            JsonLiteExtractionRule,
            id_registry,
            "json_missing_dev",
            module_name="parent",
            json_key="missing",
        )
        in_batch = _in_batch(pool, '{"key":"value"}')
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_json_lite_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        assert _rows(out_batch) == []

        in_batch.release()
        out_batch.release()


class TestDsvExtractionRule:
    def test_maps_positional_fields_by_name(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            DsvExtractionRule,
            id_registry,
            "dsv_dev",
            module_name="dsv_dev.parent",
            field_delimiter=";",
            field_names=[{"name": "f1"}, {"name": "f2", "ignore": True}, {"name": "f3"}],
        )
        in_batch = _in_batch(pool, "A;B;C")
        out_batch = _out_batch(pool)

        _base_mod_id, rule_id, cfg = rule.bundle()
        assert rule_id == KvRuleID.DSV

        nb_extract_dsv_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        f1_mod = device.get_module("parent.f1").id
        f3_mod = device.get_module("parent.f3").id
        assert (b"A", f1_mod) in rows
        assert (b"C", f3_mod) in rows
        assert not any(msg == b"B" for msg, _mod in rows)

        in_batch.release()
        out_batch.release()

    def test_startswith_gate_rejects_non_matching_lines(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            DsvExtractionRule,
            id_registry,
            "dsv_sw_dev",
            module_name="dsv_sw_dev.parent",
            field_delimiter=";",
            startswith="SIG:",
            field_names=[{"name": "f1"}],
        )
        in_batch = _in_batch(pool, "NOPE;A")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_dsv_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        assert _rows(out_batch) == []

        in_batch.release()
        out_batch.release()

    def test_prefix_strip_after_startswith(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            DsvExtractionRule,
            id_registry,
            "dsv_prefix_dev",
            module_name="dsv_prefix_dev.parent",
            field_delimiter=";",
            startswith="SIG:",
            prefix_strip=" ",
            field_names=[{"name": "f1"}],
        )
        in_batch = _in_batch(pool, "SIG: A;rest")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_dsv_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        f1_mod = device.get_module("parent.f1").id
        assert (b"A", f1_mod) in rows

        in_batch.release()
        out_batch.release()


class TestPositionalExtractionRule:
    def test_extracts_word_range_by_index_and_count(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            PositionalExtractionRule,
            id_registry,
            "pos_dev",
            module_name="pos_dev.parent",
            module_suffix="extracted",
            word_index=1,
            word_count=2,
        )
        in_batch = _in_batch(pool, "one two three four")
        out_batch = _out_batch(pool)

        _base_mod_id, rule_id, cfg = rule.bundle()
        assert rule_id == KvRuleID.POSITIONAL

        nb_extract_positional_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"two three", expected_mod) in rows

        in_batch.release()
        out_batch.release()

    def test_count_zero_extracts_to_end(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(
            PositionalExtractionRule,
            id_registry,
            "pos_all_dev",
            module_name="pos_all_dev.parent",
            module_suffix="extracted",
            word_index=2,
            word_count=0,
        )
        in_batch = _in_batch(pool, "one two three four")
        out_batch = _out_batch(pool)

        _base_mod_id, _rule_id, cfg = rule.bundle()
        nb_extract_positional_row(in_batch.bundle, 0, cfg, out_batch.bundle, device.id)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"three four", expected_mod) in rows

        in_batch.release()
        out_batch.release()


class TestWholeBatchSingleKernelCall:
    """Proves the core "whole batch processed with a single Numba function call" requirement -
    multiple rows, multiple rule types, multiple modules, resolved in exactly one
    nb_process_kv_batch invocation."""

    def test_one_call_handles_every_row_across_multiple_rule_types(self, id_registry):
        pool = NumpyArrayPool()
        device = id_registry.get_device("wholebatch_dev")
        source_device = id_registry.get_device("wholebatch_source_dev")

        kv_rule, _ = _make_rule(
            KeyValueExtractionRule, id_registry, "wholebatch_dev", module_name="wholebatch_dev.sensor"
        )
        pos_rule, _ = _make_rule(
            PositionalExtractionRule,
            id_registry,
            "wholebatch_dev",
            module_name="wholebatch_dev.other",
            module_suffix="wholebatch_dev.other.word0",
            word_index=0,
            word_count=1,
        )

        sensor_mod = device.get_module("sensor").id
        other_mod = device.get_module("other").id

        rules_raw = [kv_rule.bundle(), pos_rule.bundle()]

        from numba import typeof, types
        from numba.typed import List as NumbaList

        from blinkview.core.types.kv_extraction import EmptyKvRuleConfig

        kv_config_type = typeof(EmptyKvRuleConfig)
        kv_rule_type = types.Tuple((types.int64, types.int64, kv_config_type))
        rules = NumbaList.empty_list(kv_rule_type)
        for mod_id, rule_id, cfg in rules_raw:
            rules.append((mod_id, rule_id, cfg))

        in_batch = pool.create(PooledLogBatch, 4, 256, has_levels=True, has_modules=True, has_devices=True)
        in_batch.insert(1000, 1000, b"reading=1", module=sensor_mod, device=source_device.id)
        in_batch.insert(1001, 1001, b"one two three", module=other_mod, device=source_device.id)
        in_batch.insert(1002, 1002, b"unrelated line", module=999999, device=source_device.id)

        out_batch = _out_batch(pool, capacity=8, buffer_bytes=256)
        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()
        state = KvExtractState(in_idx=np.zeros(1, dtype=np.int64))

        out_is_full = nb_process_kv_batch(
            in_batch.bundle,
            state,
            rules,
            tracker,
            string_table,
            out_batch.bundle,
            device.id,
            id_registry.get_device("SYSTEM").id,
            device.id,
        )

        assert out_is_full is False
        assert state.in_idx[0] == 0  # fully consumed, reset for the next input batch

        resolve_temp_ids(tracker, device, out_batch)

        rows = _rows(out_batch)
        reading_mod = device.get_module("sensor.reading").id
        word0_mod = device.get_module("wholebatch_dev.other.word0").id
        assert (b"1", reading_mod) in rows
        assert (b"one", word0_mod) in rows
        # the third (unmatched-module) row contributes nothing
        assert len(rows) == 2

        in_batch.release()
        out_batch.release()

    def test_resumes_cleanly_across_a_full_output_batch(self, id_registry):
        """A too-small output batch forces nb_process_kv_batch to stop mid-input and report
        out_is_full - resuming with a fresh output batch must not re-emit or drop rows."""
        pool = NumpyArrayPool()
        device = id_registry.get_device("resume_dev")
        source_device = id_registry.get_device("resume_source_dev")

        rule, _ = _make_rule(
            PositionalExtractionRule,
            id_registry,
            "resume_dev",
            module_name="resume_dev.parent",
            module_suffix="resume_dev.parent.word",
            word_index=0,
            word_count=1,
        )
        base_mod_id, rule_id, cfg = rule.bundle()

        from numba import typeof, types
        from numba.typed import List as NumbaList

        from blinkview.core.types.kv_extraction import EmptyKvRuleConfig

        kv_config_type = typeof(EmptyKvRuleConfig)
        kv_rule_type = types.Tuple((types.int64, types.int64, kv_config_type))
        rules = NumbaList.empty_list(kv_rule_type)
        rules.append((base_mod_id, rule_id, cfg))

        parent_mod = device.get_module("parent").id

        in_batch = pool.create(PooledLogBatch, 8, 512, has_levels=True, has_modules=True, has_devices=True)
        for n in range(5):
            in_batch.insert(1000 + n, 1000 + n, f"row{n} rest".encode(), module=parent_mod, device=source_device.id)

        tracker = make_kv_tracker()
        string_table = device.modules_table.bundle()
        state = KvExtractState(in_idx=np.zeros(1, dtype=np.int64))

        # Capacity of 2 rows guarantees at least one resume cycle across 5 input rows.
        collected = []
        out_batch = _out_batch(pool, capacity=2, buffer_bytes=256)
        system_id = id_registry.get_device("SYSTEM").id

        while True:
            out_is_full = nb_process_kv_batch(
                in_batch.bundle, state, rules, tracker, string_table, out_batch.bundle, device.id, system_id, device.id
            )
            collected.extend(_rows(out_batch))
            if not out_is_full:
                out_batch.release()
                break
            out_batch.release()
            out_batch = _out_batch(pool, capacity=2, buffer_bytes=256)

        expected_word_mod = device.get_module("resume_dev.parent.word").id
        assert sorted(collected) == sorted((f"row{n}".encode(), expected_word_mod) for n in range(5))

        in_batch.release()


class QueueParser:
    def __init__(self):
        self.queue: "queue.Queue[tuple]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, _rx, level, module, *_rest in batch:
                self.queue.put((bytes(msg), int(module)))


def drain(q, count, timeout=5.0):
    items = []
    deadline = time.time() + timeout
    while len(items) < count and time.time() < deadline:
        try:
            items.append(q.get(timeout=max(0.0, deadline - time.time())))
        except queue.Empty:
            break
    return items


class TestMultiRuleKeyValueParserRealThread:
    def make_shared(self, id_registry):
        registry = FactoryRegistry()
        registry.register("key_value_rule", ExtractionRuleFactory)
        return SimpleNamespace(
            array_pool=NumpyArrayPool(),
            time_ns=time.time_ns,
            factories=registry,
            id_registry=id_registry,
        )

    def test_dispatches_matching_rows_to_the_configured_rule(self, id_registry):
        parser = MultiRuleKeyValueParser()
        parser.logger = PrintLogger("test.multi_rule_kv")
        parser.shared = self.make_shared(id_registry)
        device = id_registry.get_device("mrkv_thread_dev")
        parser_module = device.get_module("sensor")
        source_device = id_registry.get_device("mrkv_thread_source_dev")
        parser.local = SimpleNamespace(device_id=device)
        parser.apply_config(
            {
                "delay": 20,
                "rules": [
                    {
                        "type": "key_value",
                        "module_name": "mrkv_thread_dev.sensor",
                        "field_delimiter": " ",
                        "kv_delimiter": "=",
                    }
                ],
            }
        )
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        pool = parser.shared.array_pool
        batch = pool.create(PooledLogBatch, 4, 512, has_levels=True, has_modules=True, has_devices=True)
        batch.insert(1000, 1000, b"temp=25.0", module=parser_module.id, device=source_device.id)
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=1)
        finally:
            parser.stop()

        expected_mod = device.get_module("sensor.temp").id
        assert rows == [(b"25.0", expected_mod)]

    def test_rows_from_own_device_are_skipped(self, id_registry):
        parser = MultiRuleKeyValueParser()
        parser.logger = PrintLogger("test.multi_rule_kv_selfskip")
        parser.shared = self.make_shared(id_registry)
        device = id_registry.get_device("mrkv_selfskip_dev")
        parser_module = device.get_module("sensor")
        parser.local = SimpleNamespace(device_id=device)
        parser.apply_config(
            {
                "delay": 20,
                "rules": [
                    {
                        "type": "key_value",
                        "module_name": "mrkv_selfskip_dev.sensor",
                    }
                ],
            }
        )
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        pool = parser.shared.array_pool
        batch = pool.create(PooledLogBatch, 4, 512, has_levels=True, has_modules=True, has_devices=True)
        batch.insert(1000, 1000, b"x=1", module=parser_module.id, device=device.id)
        parser.put(batch)

        parser.start()
        try:
            time.sleep(0.3)
        finally:
            parser.stop()

        assert subscriber.queue.empty()

    def test_multiple_rules_across_multiple_rows_in_one_go(self, id_registry):
        """Real end-to-end thread test with several rows spanning different configured rules,
        proving the real run() loop (single kernel call per input chunk) dispatches correctly."""
        parser = MultiRuleKeyValueParser()
        parser.logger = PrintLogger("test.multi_rule_kv_multi")
        parser.shared = self.make_shared(id_registry)
        device = id_registry.get_device("mrkv_multi_dev")
        sensor_mod = device.get_module("sensor")
        other_mod = device.get_module("other")
        source_device = id_registry.get_device("mrkv_multi_source_dev")
        parser.local = SimpleNamespace(device_id=device)
        parser.apply_config(
            {
                "delay": 20,
                "rules": [
                    {"type": "key_value", "module_name": "mrkv_multi_dev.sensor"},
                    {
                        "type": "positional",
                        "module_name": "mrkv_multi_dev.other",
                        "module_suffix": "mrkv_multi_dev.other.first_word",
                        "word_index": 0,
                        "word_count": 1,
                    },
                ],
            }
        )
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        pool = parser.shared.array_pool
        batch = pool.create(PooledLogBatch, 4, 512, has_levels=True, has_modules=True, has_devices=True)
        batch.insert(1000, 1000, b"temp=25.0", module=sensor_mod.id, device=source_device.id)
        batch.insert(1001, 1001, b"hello world", module=other_mod.id, device=source_device.id)
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=2)
        finally:
            parser.stop()

        temp_mod = device.get_module("sensor.temp").id
        first_word_mod = device.get_module("mrkv_multi_dev.other.first_word").id
        assert sorted(rows) == sorted([(b"25.0", temp_mod), (b"hello", first_word_mod)])
