# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import LEVEL_TYPE, SEQ_NONE
from blinkview.core.id_registry.registry import IDRegistry
from blinkview.core.log_fetch import LogSegmentScanner, LogTextFetcher
from blinkview.core.types.formatting import FormattingConfig
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from tests.fakes.devices import esp32_wifi
from tests.fakes.log_bundle import make_log_bundle as make_bundle
from tests.fakes.log_pool import FakeIndicesHandle, FakeLogPool, FakeSegment


def _permissive_sidebar():
    return False, np.zeros(0, dtype=np.uint8)


def make_scanner(id_registry, log_pool, log_filter, *, show_hidden=True):
    return LogSegmentScanner(
        id_registry,
        lambda: log_pool,
        log_filter,
        get_sidebar_filter=_permissive_sidebar,
        get_show_hidden=lambda: show_hidden,
    )


class RecordingConsumer:
    """Trivial consume callback recording each (segment, matched_seqs) call, for scan-mechanics
    tests that don't care about a real terminal format."""

    def __init__(self):
        self.calls = []

    def __call__(self, segment, indices_array, match_count):
        seqs = segment.bundle.sequences[indices_array[:match_count]].copy()
        self.calls.append((segment, seqs))

    @property
    def all_seqs(self):
        return [int(s) for _, seqs in self.calls for s in seqs]


class TestScanTail:
    def test_basic_match_chronological_order_and_watermark(self, id_registry, log_filter):
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[LogLevel.INFO.value] * 3,
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["a", "b", "c"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        scanner = make_scanner(id_registry, pool, log_filter)

        consumer = RecordingConsumer()
        result = scanner.scan_tail(start_seq=SEQ_NONE, max_rows=10, consume=consumer)

        assert result.total_new_rows == 3
        assert result.reached_live_edge is True
        assert result.highest_seq_seen == 3
        # consume is invoked in scan order (newest segment/row block first); within a single
        # segment's match_count the kernel itself yields chronological (ascending seq) order.
        assert consumer.all_seqs == [1, 2, 3]

    def test_no_new_data_past_watermark_scans_nothing(self, id_registry, log_filter):
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[LogLevel.INFO.value] * 3,
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["a", "b", "c"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        scanner = make_scanner(id_registry, pool, log_filter)

        result = scanner.scan_tail(start_seq=3, max_rows=10, consume=RecordingConsumer())

        assert result.total_new_rows == 0
        assert result.highest_seq_seen == 3  # unchanged watermark: no segment passed the check

    def test_capped_by_max_rows_reports_not_reached_live_edge(self, id_registry, log_filter):
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=list(range(10)),
            devices=[device.id] * 10,
            levels=[LogLevel.INFO.value] * 10,
            modules=[module.id] * 10,
            sequences=list(range(1, 11)),
            messages=[f"m{i}" for i in range(10)],
        )
        pool = FakeLogPool(latest_seq=10, segments=[FakeSegment(bundle)])
        scanner = make_scanner(id_registry, pool, log_filter)

        consumer = RecordingConsumer()
        result = scanner.scan_tail(start_seq=SEQ_NONE, max_rows=4, consume=consumer)

        assert result.total_new_rows == 4
        assert result.reached_live_edge is False
        # The newest 4 rows should be kept (a live tail keeps the newest matches under a cap).
        assert consumer.all_seqs == [7, 8, 9, 10]

    def test_kv_and_text_filter_are_applied(self, id_registry, log_filter):
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[LogLevel.INFO.value] * 3,
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["status=ok id=1", "status=fail id=2", "status=ok id=3"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        scanner = make_scanner(id_registry, pool, log_filter)

        log_filter.set_kv_filter("status=ok")
        consumer = RecordingConsumer()
        result = scanner.scan_tail(start_seq=SEQ_NONE, max_rows=10, consume=consumer)

        assert result.total_new_rows == 2
        assert consumer.all_seqs == [1, 3]

    def test_module_filter_with_descendant_expansion(self, id_registry):
        device = id_registry.get_device("esp32")
        wifi = device.get_module("wifi")
        ble = device.get_module("ble")

        log_filter = LogFilter(id_registry, log_level=LogLevel.ALL.name_conf, filtered_module=wifi)
        bundle = make_bundle(
            timestamps=[1, 2],
            devices=[device.id, device.id],
            levels=[LogLevel.INFO.value, LogLevel.INFO.value],
            modules=[wifi.id, ble.id],
            sequences=[1, 2],
            messages=["wifi log", "ble log"],
        )
        pool = FakeLogPool(latest_seq=2, segments=[FakeSegment(bundle)])
        scanner = make_scanner(id_registry, pool, log_filter)

        consumer = RecordingConsumer()
        result = scanner.scan_tail(start_seq=SEQ_NONE, max_rows=10, consume=consumer)

        assert result.total_new_rows == 1
        assert consumer.all_seqs == [1]

    def test_permissive_sidebar_and_show_hidden_degenerates_to_flat_level_mask(self, id_registry):
        """Protects the ConsoleSubscriber migration's equivalence claim: get_sidebar_filter
        always (False, empty) + get_show_hidden always True must reduce to a flat
        level-threshold-only mask, matching ConsoleSubscriber's original
        np.full(mod_count, log_level) one-liner."""
        device, module = esp32_wifi(id_registry)
        log_filter = LogFilter(id_registry, log_level=LogLevel.WARN.name_conf)
        bundle = make_bundle(
            timestamps=[1, 2],
            devices=[device.id, device.id],
            levels=[LogLevel.INFO.value, LogLevel.WARN.value],
            modules=[module.id, module.id],
            sequences=[1, 2],
            messages=["info", "warn"],
        )
        pool = FakeLogPool(latest_seq=2, segments=[FakeSegment(bundle)])
        scanner = make_scanner(id_registry, pool, log_filter, show_hidden=True)

        mod_count = scanner.ensure_effective_mask()
        expected = np.full(mod_count, LEVEL_TYPE(log_filter.log_level.value), dtype=LEVEL_TYPE)
        assert np.array_equal(scanner._effective_mask, expected)

        consumer = RecordingConsumer()
        result = scanner.scan_tail(start_seq=SEQ_NONE, max_rows=10, consume=consumer)
        assert result.total_new_rows == 1  # only the WARN row passes a WARN threshold
        assert consumer.all_seqs == [2]


class TestScanHistoryWindow:
    def _make_scanner(self, id_registry, log_filter, count=20):
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=list(range(count)),
            devices=[device.id] * count,
            levels=[LogLevel.INFO.value] * count,
            modules=[module.id] * count,
            sequences=list(range(1, count + 1)),
            messages=[f"m{i}" for i in range(count)],
        )
        pool = FakeLogPool(latest_seq=count, segments=[FakeSegment(bundle)])
        return make_scanner(id_registry, pool, log_filter)

    def test_seq_anchor_before_after_and_reached_start(self, id_registry, log_filter):
        scanner = self._make_scanner(id_registry, log_filter, count=20)

        before = RecordingConsumer()
        after = RecordingConsumer()
        result = scanner.scan_history_window(
            anchor_seq=10, before_cap=500, after_cap=500, consume_before=before, consume_after=after
        )

        assert result.before_count == 9  # seqs 1..9
        assert result.after_count == 11  # seqs 10..20
        assert result.oldest_seq == 1
        assert result.newest_seq == 20
        assert result.reached_start is True
        # segment_filter_reversed scans newest-to-oldest internally but already yields
        # chronological (ascending) order per segment/call - with a single segment here, both
        # "before" and "after" consumer calls see ascending seqs; multi-segment ordering is what
        # the caller's own batch-list-reversal (LogTextFetcher) handles.
        assert before.all_seqs == list(range(1, 10))
        assert after.all_seqs == list(range(10, 21))

    def test_ts_anchor_asymmetric_inclusive_bounds(self, id_registry, log_filter):
        scanner = self._make_scanner(id_registry, log_filter, count=10)

        before = RecordingConsumer()
        after = RecordingConsumer()
        # timestamps equal row index (0..9); anchor at ts=5 should exclude seq for ts=5 from
        # "before" and include it in "after".
        result = scanner.scan_history_window(
            anchor_ts=5, before_cap=500, after_cap=500, consume_before=before, consume_after=after
        )

        assert 6 not in before.all_seqs  # seq for ts=5 is 6 (seqs are 1-indexed)
        assert 6 in after.all_seqs

    def test_reached_start_false_when_full_page_within_one_segment(self, id_registry, log_filter):
        scanner = self._make_scanner(id_registry, log_filter, count=20)

        before = RecordingConsumer()
        after = RecordingConsumer()
        result = scanner.scan_history_window(
            anchor_seq=10, before_cap=3, after_cap=3, consume_before=before, consume_after=after
        )

        assert result.before_count == 3
        assert result.reached_start is False  # hit quota exactly - must not falsely claim start


class TestLogTextFetcher:
    def _make_fetcher(self, id_registry, array_pool, log_pool, log_filter):
        scanner = make_scanner(id_registry, log_pool, log_filter)
        return LogTextFetcher(scanner, array_pool, tables_provider=id_registry.bundle)

    def test_fetch_live_tail_produces_chronological_formatted_text(self, id_registry, array_pool, log_filter):
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[LogLevel.INFO.value] * 3,
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["first", "second", "third"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        fetcher = self._make_fetcher(id_registry, array_pool, pool, log_filter)

        format_cfg = FormattingConfig(True, True, True, True)
        result = fetcher.fetch_live_tail(max_rows=10, format_cfg=format_cfg, tz_offset_sec=0)

        assert result.total_new_rows == 3
        assert list(result.seqs) == [1, 2, 3]
        lines = [ln for ln in result.text.split("\n") if ln]
        assert len(lines) == 3
        assert "first" in lines[0]
        assert "second" in lines[1]
        assert "third" in lines[2]
        assert fetcher.latest_seq_seen == 3

    def test_fetch_history_window_spans_before_after_boundary(self, id_registry, array_pool, log_filter):
        device, module = esp32_wifi(id_registry)
        count = 10
        bundle = make_bundle(
            timestamps=list(range(count)),
            devices=[device.id] * count,
            levels=[LogLevel.INFO.value] * count,
            modules=[module.id] * count,
            sequences=list(range(1, count + 1)),
            messages=[f"row{i}" for i in range(count)],
        )
        pool = FakeLogPool(latest_seq=count, segments=[FakeSegment(bundle)])
        fetcher = self._make_fetcher(id_registry, array_pool, pool, log_filter)

        format_cfg = FormattingConfig(True, True, True, True)
        result = fetcher.fetch_history_window(
            anchor_seq=5, before_cap=500, after_cap=500, format_cfg=format_cfg, tz_offset_sec=0
        )

        assert result.before_count == 4
        assert result.after_count == 6
        lines = [ln for ln in result.text.split("\n") if ln]
        assert lines[0].endswith("row0")
        assert lines[-1].endswith("row9")

    def test_chained_live_then_history_agree_on_overlapping_row(self, id_registry, array_pool, log_filter):
        """End-to-end chained scenario: ensure_effective_mask -> fetch_live_tail ->
        fetch_history_window against the same pool/registry, asserting a row visible in the
        live tail also appears correctly in a history window anchored just past it - guards the
        scanner/wrapper split introduced by this refactor (per project convention: a chained
        real-kernel test catches dropped-at-a-seam bugs isolated per-stage tests miss)."""
        device, module = esp32_wifi(id_registry)
        bundle = make_bundle(
            timestamps=[1, 2, 3],
            devices=[device.id] * 3,
            levels=[LogLevel.INFO.value] * 3,
            modules=[module.id] * 3,
            sequences=[1, 2, 3],
            messages=["alpha", "beta", "gamma"],
        )
        pool = FakeLogPool(latest_seq=3, segments=[FakeSegment(bundle)])
        fetcher = self._make_fetcher(id_registry, array_pool, pool, log_filter)

        fetcher.ensure_effective_mask()
        format_cfg = FormattingConfig(True, True, True, True)
        live_result = fetcher.fetch_live_tail(max_rows=10, format_cfg=format_cfg, tz_offset_sec=0)
        assert "beta" in live_result.text

        history_result = fetcher.fetch_history_window(
            anchor_seq=3, before_cap=500, after_cap=500, format_cfg=format_cfg, tz_offset_sec=0
        )
        assert "beta" in history_result.text
        assert history_result.before_count == 2  # seq 1, 2
        assert history_result.after_count == 1  # seq 3 (the anchor)


class FakeWarmupHelper:
    """Duck-typed stand-in for NumbaWarmupHelper exposing only what
    LogSegmentScanner.warmup/LogTextFetcher.warmup touch (registry/log_pool/array_pool/
    floats_mod/warmup_mod) - building a real NumbaWarmupHelper requires a full SystemContext
    (tasks/settings/factories) that isn't needed just to smoke-test these two callbacks."""

    def __init__(self):
        self.array_pool = NumpyArrayPool()
        self.registry = IDRegistry(self.array_pool)
        self.warmup_mod = self.registry.resolve_module("numba.warmup")
        self.floats_mod = self.registry.resolve_module("tool.floats")

        bundle = make_bundle(
            timestamps=[1, 2],
            devices=[0, 0],
            levels=[LogLevel.ALL.value, LogLevel.ALL.value],
            modules=[self.warmup_mod.id, self.floats_mod.id],
            sequences=[1, 2],
            messages=["1.0 2.0", "3.0"],
        )
        self.log_pool = FakeLogPool(latest_seq=2, segments=[FakeSegment(bundle)])


def test_warmup_smoke():
    """Confirms LogSegmentScanner.warmup/LogTextFetcher.warmup run without error against a
    minimal fake NumbaWarmupHelper-shaped environment."""
    helper = FakeWarmupHelper()
    LogSegmentScanner.warmup(helper)
    LogTextFetcher.warmup(helper)
