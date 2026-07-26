# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.parsing import (
    EmptyUnifiedParserState,
    UnifiedParserConfig,
    create_default_sync,
    prime_sync_state,
)
from blinkview.ops.timestamps import (
    nb_apply_drift_projection,
    nb_auto_sync_fallback,
    nb_auto_sync_fallback_2,
    nb_parse_int_timestamp,
    nb_project_synced_ns,
)


def _out_bundle(capacity=1):
    return LogBundle(
        timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        offsets=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        buffer=np.zeros(capacity * 32, dtype=dtypes.BYTE),
        levels=np.zeros(capacity, dtype=dtypes.LEVEL_TYPE),
        modules=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        devices=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        sequences=np.zeros(capacity, dtype=dtypes.SEQ_TYPE),
        pids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        tids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        ext_u32_1=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u32_2=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u64_1=np.zeros(capacity, dtype=dtypes.UINT64),
        size=np.array([0], dtype=np.int64),
        msg_cursor=np.array([0], dtype=np.int64),
        capacity=capacity,
        has_levels=False,
        has_modules=False,
        has_devices=False,
        has_sequences=False,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def _buf(msg):
    return np.frombuffer(msg.encode("ascii"), dtype=dtypes.BYTE)


class TestParseIntTimestamp:
    def test_seconds_precision_unix_mode_writes_raw_seconds_as_ns(self):
        out_b = _out_bundle()
        config = UnifiedParserConfig(timestamp_precision=0, timestamp_unix=True)

        next_cursor = nb_parse_int_timestamp(_buf("5 rest"), 0, 6, out_b, 0, EmptyUnifiedParserState, config)

        assert int(out_b.timestamps[0]) == 5_000_000_000
        assert _buf("5 rest")[next_cursor : next_cursor + 1].tobytes() == b"r"

    def test_millis_precision(self):
        out_b = _out_bundle()
        config = UnifiedParserConfig(timestamp_precision=1, timestamp_unix=True)

        nb_parse_int_timestamp(_buf("1500"), 0, 4, out_b, 0, EmptyUnifiedParserState, config)

        assert int(out_b.timestamps[0]) == 1_500_000_000

    def test_no_digits_returns_negative_one(self):
        out_b = _out_bundle()
        config = UnifiedParserConfig(timestamp_precision=0, timestamp_unix=True)

        result = nb_parse_int_timestamp(_buf("abc"), 0, 3, out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1

    def test_undefined_precision_returns_negative_one(self):
        out_b = _out_bundle()
        config = UnifiedParserConfig(timestamp_precision=9, timestamp_unix=True)

        result = nb_parse_int_timestamp(_buf("5"), 0, 1, out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1

    def test_non_unix_mode_projects_through_sync_state(self):
        out_b = _out_bundle()
        out_b.rx_timestamps[0] = 10_000_000_000
        config = UnifiedParserConfig(timestamp_precision=0, timestamp_unix=False)

        nb_parse_int_timestamp(_buf("1"), 0, 1, out_b, 0, EmptyUnifiedParserState, config)

        # timestamp_unix=False routes through nb_project_synced_ns; with the default (disabled)
        # sync state this falls back to nb_auto_sync_fallback, whose first-ever call just
        # anchors and echoes rx_ns straight back.
        assert int(out_b.timestamps[0]) == 10_000_000_000


class TestApplyDriftProjection:
    def test_identity_drift_matches_delta_exactly(self):
        result = nb_apply_drift_projection(
            raw_ns=2_000_000_000,
            anchor_raw=1_000_000_000,
            anchor_rx=5_000_000_000,
            drift_m=1_000_000_000,
            drift_d=1_000_000_000,
        )
        assert int(result) == 6_000_000_000

    def test_drift_scales_the_delta(self):
        # 2x drift: every raw ns elapsed maps to 2 rx ns elapsed.
        result = nb_apply_drift_projection(
            raw_ns=2_000_000_000, anchor_raw=1_000_000_000, anchor_rx=0, drift_m=2_000_000_000, drift_d=1_000_000_000
        )
        assert int(result) == 2_000_000_000


class TestAutoSyncFallback:
    def test_first_call_initializes_anchor_and_returns_rx_ns(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)

        result = nb_auto_sync_fallback(raw_ns=1000, rx_ns=5_000_000_000, sync=sync)

        assert int(result) == 5_000_000_000
        assert sync.auto_init[0] == 1
        assert sync.auto_last_raw[0] == 1000

    def test_second_call_projects_using_fixed_offset(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)
        nb_auto_sync_fallback(raw_ns=0, rx_ns=1_000_000_000, sync=sync)

        result = nb_auto_sync_fallback(raw_ns=500_000_000, rx_ns=1_600_000_000, sync=sync)

        # anchor_offset = 1_000_000_000 - 0 = 1_000_000_000; projected = 500_000_000 + offset
        assert int(result) == 1_500_000_000

    def test_monotonicity_guard_never_goes_backward(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)
        nb_auto_sync_fallback(raw_ns=0, rx_ns=1_000_000_000, sync=sync)
        first = nb_auto_sync_fallback(raw_ns=500_000_000, rx_ns=1_600_000_000, sync=sync)

        # Same raw_ns again (out-of-order) must not project backward past `first`.
        second = nb_auto_sync_fallback(raw_ns=500_000_000, rx_ns=1_600_000_000, sync=sync)

        assert int(second) > int(first)


class TestAutoSyncFallback2:
    def test_first_call_initializes_and_returns_rx_ns(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)

        result = nb_auto_sync_fallback_2(raw_ns=1000, rx_ns=5_000_000_000, sync=sync)

        assert int(result) == 5_000_000_000
        assert sync.auto_init[0] == 1

    def test_reboot_detected_when_raw_ns_goes_backward(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)
        nb_auto_sync_fallback_2(raw_ns=10_000_000_000, rx_ns=10_000_000_000, sync=sync)

        # A much smaller raw_ns than last seen looks like a device reboot - re-anchors.
        result = nb_auto_sync_fallback_2(raw_ns=0, rx_ns=20_000_000_000, sync=sync)

        assert int(result) == 20_000_000_000
        assert sync.auto_last_raw[0] == 0


class TestProjectSyncedNs:
    def test_disabled_sync_uses_auto_fallback(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)

        result = nb_project_synced_ns(raw_ns=1000, rx_ns=5_000_000_000, sync=sync)

        assert int(result) == 5_000_000_000

    def test_enabled_sync_uses_drift_projection(self):
        sync = create_default_sync(now_ns=1_000_000_000, start_enabled=True)

        result = nb_project_synced_ns(raw_ns=1_000_000_000, rx_ns=0, sync=sync)

        # start_enabled anchors offset == ref_time == now_ns, identity drift -> raw echoes back.
        assert int(result) == 1_000_000_000

    def test_primed_sync_state_is_used_when_enabled(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)
        prime_sync_state(sync, phone_ns=1_000_000_000, pc_ns=9_000_000_000)

        result = nb_project_synced_ns(raw_ns=1_000_000_000, rx_ns=0, sync=sync)

        assert int(result) == 9_000_000_000
