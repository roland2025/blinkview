# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
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
    """Mirrors BaseFactory.build(): hydrate the schema defaults into the config, then apply -
    these classes' __init__ methods don't call super().__init__(), so plain construction never
    hydrates schema defaults on its own (same pattern used for FrameDecoder/FrameSectionParser)."""
    hydrated = instance.hydrate_config(overrides)
    instance.apply_config(hydrated)
    return instance


def _make_local_ctx(pool, message: str, level=0):
    """Builds a single-row input batch and wraps its columns as the memoryviews
    MultiRuleKeyValueParser.run() sets on self.local before calling a rule's process_fn - so a
    rule's process() can be exercised directly without spinning up the parser's thread."""
    batch = pool.create(PooledLogBatch, 4, 512, has_levels=True)
    batch.insert(1000, 1000, message.encode("ascii"), level=level)
    b = batch.bundle
    ctx = SimpleNamespace(
        buffer_mv=memoryview(b.buffer),
        offsets_mv=memoryview(b.offsets),
        lengths_mv=memoryview(b.lengths),
        timestamps_mv=memoryview(b.timestamps),
        rx_timestamps_mv=memoryview(b.rx_timestamps),
        levels_mv=memoryview(b.levels),
    )
    return ctx, batch


def _make_rule(rule_cls, id_registry, device_name, **overrides):
    rule = configure(rule_cls(), **overrides)
    device = id_registry.get_device(device_name)
    rule.shared = SimpleNamespace(id_registry=id_registry)
    return rule, device


def _out_batch(pool):
    return pool.create(PooledLogBatch, 8, 512, has_levels=True, has_modules=True, has_devices=True)


def _rows(batch):
    return [(bytes(msg), int(module)) for _ts, msg, _rx, level, module, *_rest in batch]


class TestKeyValueExtractionRule:
    def test_extracts_simple_key_value_pairs(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(KeyValueExtractionRule, id_registry, "kv_dev", module_name="kv_dev.parent")
        ctx, in_batch = _make_local_ctx(pool, "voltage=3.3 current=1.2")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "Data: level=high")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, 'msg="hello world" done=1')
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "a=1")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "a:1;b:2")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

        rows = _rows(out_batch)
        a_mod = device.get_module("parent.a").id
        b_mod = device.get_module("parent.b").id
        assert (b"1", a_mod) in rows
        assert (b"2", b_mod) in rows

        in_batch.release()
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
        ctx, in_batch = _make_local_ctx(pool, "info TOKEN abc def ghi")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "info TOKEN abc")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "info TOKEN abc")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "info TOKEN abc")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "info TOKEN abc def ghi")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"TOKEN abc def ghi", expected_mod) in rows

        in_batch.release()
        out_batch.release()


class TestJsonLiteExtractionRule:
    def test_extracts_quoted_string_value(self, id_registry):
        pool = NumpyArrayPool()
        rule, device = _make_rule(JsonLiteExtractionRule, id_registry, "json_dev", module_name="parent", json_key="key")
        ctx, in_batch = _make_local_ctx(pool, '{"key":"value","other":1}')
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, '{"count":42,"x":1}')
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, '{"key":"value"}')
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, '{"key":"value"}')
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "A;B;C")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

        rows = _rows(out_batch)
        f1_mod = device.get_module("parent.f1").id
        f3_mod = device.get_module("parent.f3").id
        assert (b"A", f1_mod) in rows
        assert (b"C", f3_mod) in rows
        assert not any(msg == b"B" for msg, _mod in rows)  # ignored field never inserted

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
        ctx, in_batch = _make_local_ctx(pool, "NOPE;A")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "SIG: A;rest")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "one two three four")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

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
        ctx, in_batch = _make_local_ctx(pool, "one two three four")
        rule.local = SimpleNamespace(device_id=device, parser_local=ctx)

        base_mod_id, process = rule.bundle()
        out_batch = _out_batch(pool)
        process(0, base_mod_id, out_batch)

        rows = _rows(out_batch)
        expected_mod = device.get_module("extracted").id
        assert (b"three four", expected_mod) in rows

        in_batch.release()
        out_batch.release()


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
        """The hot loop's dispatch table is keyed on module_id, but rows tagged with this
        parser's own device_id (or SYSTEM) are explicitly skipped regardless of module match -
        see test_rows_from_own_device_are_skipped. This parser's local.device_id is where it
        *creates* derived sub-modules, not necessarily the device its input rows are tagged
        with, so a real dispatch needs the row tagged with some other, upstream source device."""
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
        """The hot loop explicitly skips rows tagged with this parser's own device_id (or
        SYSTEM) - a safety net against a rule accidentally feeding its own output back through
        itself."""
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
        # Tagged with THIS parser's own device_id - must be skipped even though the module_id
        # would otherwise match a configured rule.
        batch.insert(1000, 1000, b"x=1", module=parser_module.id, device=device.id)
        parser.put(batch)

        parser.start()
        try:
            time.sleep(0.3)  # give the (empty, filtered) batch a moment to be processed
        finally:
            parser.stop()

        assert subscriber.queue.empty()
