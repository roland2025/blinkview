# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Process-name filtering (`process=` in the kv box, and the free-text search) and the text
viewer's process column, end to end: real Registry, real CircularLogPool segments, real
pid_history, real scan/format kernels - the same LogSegmentScanner/LogTextFetcher the viewers use."""

import numpy as np
import pytest

from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.log_fetch import LogSegmentScanner, LogTextFetcher
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.formatting import FormattingConfig
from blinkview.ops.process_filter import EMPTY_PROC_FILTER
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_registry

APP_PID = 1234
OTHER_PID = 55
UNKNOWN_PID = 777  # never in pid_history


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "process_name_filter_test")
    yield reg
    reg.stop()


@pytest.fixture
def phone(registry):
    device = registry.id_registry.get_device("phone")
    return device, device.get_module("tag")


def _key(device, pid):
    return (device.id << 32) | pid


def _push(registry, device, module, rows):
    """rows: [(ts, pid, message)]"""
    src = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(rows), 4096, has_levels=True, has_modules=True, has_devices=True, has_pids=True
    )
    with src:
        for ts, pid, msg in rows:
            src.insert_any(ts, ts, msg.encode(), level=LogLevel.INFO.value, module=module.id, device=device.id, pid=pid)
        registry.central.log_pool.batch_append(src)


def _scanner(registry, log_filter):
    return LogSegmentScanner(
        registry.id_registry,
        lambda: registry.central.log_pool,
        log_filter,
        get_sidebar_filter=lambda: (False, np.zeros(0, dtype=np.uint8)),
        get_show_hidden=lambda: True,
        get_pid_history=lambda: registry.pid_history,
    )


def _fetch_lines(registry, log_filter, format_cfg=None):
    fetcher = LogTextFetcher(
        _scanner(registry, log_filter),
        registry.system_ctx.array_pool,
        tables_provider=registry.id_registry.bundle,
        procs_provider=registry.pid_history.table,
    )
    fetcher.latest_seq_seen = SEQ_NONE
    cfg = format_cfg or FormattingConfig(show_ts=False, show_dev=False, show_lvl=False, show_mod=False)
    result = fetcher.fetch_live_tail(max_rows=100, format_cfg=cfg, tz_offset_sec=0)
    return result.text.splitlines()


def _messages(registry, *, kv="", text=""):
    log_filter = LogFilter(registry.id_registry)
    log_filter.set_kv_filter(kv)
    log_filter.set_text_filter(text)
    return _fetch_lines(registry, log_filter)


@pytest.fixture
def populated(registry, phone):
    device, module = phone
    history = registry.pid_history
    history.update(_key(device, APP_PID), "com.example.app", 1_000)
    history.update(_key(device, OTHER_PID), "system_server", 1_000)
    _push(
        registry,
        device,
        module,
        [
            (2_000, APP_PID, "app started"),
            (2_001, OTHER_PID, "server tick mentions com.example.app"),
            (2_002, UNKNOWN_PID, "from an unknown pid"),
            (2_003, 0, "no pid at all"),
            (2_004, APP_PID, "app status=ok"),
        ],
    )
    return device, module


class TestKvProcessCondition:
    def test_process_condition_keeps_only_that_process(self, registry, populated):
        assert _messages(registry, kv="process=com.example.app") == ["app started", "app status=ok"]

    def test_is_case_insensitive_substring(self, registry, populated):
        assert _messages(registry, kv="process=EXAMPLE") == ["app started", "app status=ok"]

    def test_ands_with_other_kv_conditions(self, registry, populated):
        assert _messages(registry, kv="process=example status=ok") == ["app status=ok"]

    def test_multiple_process_terms_must_all_match(self, registry, populated):
        assert _messages(registry, kv="process=com process=server") == []
        assert _messages(registry, kv="process=system process=server") == ["server tick mentions com.example.app"]

    def test_no_matching_process_matches_nothing(self, registry, populated):
        assert _messages(registry, kv="process=does.not.exist") == []

    def test_key_is_case_insensitive(self, registry, populated):
        assert _messages(registry, kv="Process=com.example.app") == ["app started", "app status=ok"]

    def test_process_is_not_also_required_as_a_message_key(self, registry):
        log_filter = LogFilter(registry.id_registry)
        log_filter.set_kv_filter("process=app status=ok")

        assert log_filter.kv_conditions == [(b"status", b"ok")]
        assert log_filter.process_terms == ["app"]

    def test_combines_with_text_search(self, registry, populated):
        assert _messages(registry, kv="process=example", text="status") == ["app status=ok"]


class TestTextSearchMatchesProcess:
    def test_search_box_matches_process_name(self, registry, populated):
        """ORed like device/module names: the app's own rows plus the server line that mentions it."""
        assert _messages(registry, text="example.app") == [
            "app started",
            "server tick mentions com.example.app",
            "app status=ok",
        ]

    def test_search_still_matches_message_text(self, registry, populated):
        assert _messages(registry, text="unknown pid") == ["from an unknown pid"]


class TestHistoryTiming:
    def test_row_before_the_process_start_does_not_match(self, registry, phone):
        device, module = phone
        registry.pid_history.update(_key(device, APP_PID), "com.example.app", 5_000)
        _push(registry, device, module, [(4_000, APP_PID, "before"), (6_000, APP_PID, "after")])

        assert _messages(registry, kv="process=example") == ["after"]

    def test_backdated_start_covers_rows_before_the_poll(self, registry, phone):
        device, module = phone
        registry.pid_history.update(_key(device, APP_PID), "com.example.app", 5_000, started_ns=3_000)
        _push(registry, device, module, [(2_000, APP_PID, "too early"), (4_000, APP_PID, "startup")])

        assert _messages(registry, kv="process=example") == ["startup"]

    def test_reused_pid_matches_only_its_own_interval(self, registry, phone):
        device, module = phone
        history = registry.pid_history
        history.update(_key(device, APP_PID), "first.app", 1_000)
        history.update(_key(device, APP_PID), "second.app", 3_000)
        _push(registry, device, module, [(2_000, APP_PID, "one"), (4_000, APP_PID, "two")])

        assert _messages(registry, kv="process=first") == ["one"]
        assert _messages(registry, kv="process=second") == ["two"]

    def test_same_pid_on_another_device_is_a_different_process(self, registry, phone):
        device, module = phone
        tablet = registry.id_registry.get_device("tablet")
        registry.pid_history.update(_key(device, APP_PID), "com.example.app", 1_000)
        _push(registry, device, module, [(2_000, APP_PID, "phone row")])
        _push(registry, tablet, tablet.get_module("tag"), [(2_001, APP_PID, "tablet row")])

        assert _messages(registry, kv="process=example") == ["phone row"]

    def test_filter_picks_up_a_process_learned_after_baking(self, registry, phone):
        """The baked filter is keyed on the history's version: a poll that names a new process
        must reach the next fetch without the user retyping the filter."""
        device, module = phone
        log_filter = LogFilter(registry.id_registry)
        log_filter.set_kv_filter("process=late")
        _push(registry, device, module, [(2_000, APP_PID, "late row")])
        assert _fetch_lines(registry, log_filter) == []

        registry.pid_history.update(_key(device, APP_PID), "late.app", 3_000, started_ns=1_000)

        assert _fetch_lines(registry, log_filter) == ["late row"]


class TestBakeIdentity:
    def test_inactive_filter_bakes_to_the_empty_singleton(self, registry, populated):
        log_filter = LogFilter(registry.id_registry)
        assert log_filter.bake_process_filter(registry.pid_history) is EMPTY_PROC_FILTER

    def test_active_filter_is_cached_until_something_changes(self, registry, populated):
        device, _ = populated
        log_filter = LogFilter(registry.id_registry)
        log_filter.set_kv_filter("process=example")
        first = log_filter.bake_process_filter(registry.pid_history)
        assert log_filter.bake_process_filter(registry.pid_history) is first

        registry.pid_history.update(_key(device, 999), "new.proc", 9_000)
        assert log_filter.bake_process_filter(registry.pid_history) is not first

    def test_missing_history_with_a_process_condition_matches_nothing(self, registry, populated):
        log_filter = LogFilter(registry.id_registry)
        log_filter.set_kv_filter("process=example")
        scanner = LogSegmentScanner(
            registry.id_registry,
            lambda: registry.central.log_pool,
            log_filter,
            get_sidebar_filter=lambda: (False, np.zeros(0, dtype=np.uint8)),
            get_show_hidden=lambda: True,
        )
        rows = []
        scanner.scan_tail(start_seq=SEQ_NONE, max_rows=100, consume=lambda seg, idx, n: rows.append(n))
        assert rows == []


class TestProcessColumn:
    def test_text_lines_show_process_between_level_and_module(self, registry, populated):
        cfg = FormattingConfig(show_ts=False, show_dev=True, show_lvl=True, show_mod=True, show_proc=True)
        lines = _fetch_lines(registry, LogFilter(registry.id_registry), cfg)

        assert lines == [
            "phone I com.example.app tag: app started",
            "phone I system_server tag: server tick mentions com.example.app",
            "phone I - tag: from an unknown pid",
            "phone I - tag: no pid at all",
            "phone I com.example.app tag: app status=ok",
        ]

    def test_process_column_off_by_default(self, registry, populated):
        cfg = FormattingConfig(show_ts=False, show_dev=False, show_lvl=False, show_mod=True)
        lines = _fetch_lines(registry, LogFilter(registry.id_registry), cfg)

        assert lines[0] == "tag: app started"

    def test_process_column_alone(self, registry, populated):
        cfg = FormattingConfig(show_ts=False, show_dev=False, show_lvl=False, show_mod=False, show_proc=True)
        lines = _fetch_lines(registry, LogFilter(registry.id_registry), cfg)

        assert lines[0] == "com.example.app app started"
