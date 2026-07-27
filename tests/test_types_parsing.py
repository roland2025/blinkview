# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.types.parsing import AUTO_WARMUP_VAL, create_default_sync, prime_sync_state


class TestCreateDefaultSync:
    def test_disabled_by_default(self):
        sync = create_default_sync(now_ns=1_000_000_000, start_enabled=False)
        assert sync.enabled[0] == 0

    def test_disabled_offset_is_zero_not_anchored(self):
        sync = create_default_sync(now_ns=1_000_000_000, start_enabled=False)
        assert list(sync.offset) == [0, 0]

    def test_start_enabled_marks_enabled(self):
        sync = create_default_sync(now_ns=1_000_000_000, start_enabled=True)
        assert sync.enabled[0] == 1

    def test_start_enabled_anchors_offset_to_now_to_avoid_epoch_bug(self):
        now_ns = 1_700_000_000_000_000_000
        sync = create_default_sync(now_ns=now_ns, start_enabled=True)
        assert list(sync.offset) == [now_ns, now_ns]

    def test_ref_time_is_always_anchored_to_now(self):
        now_ns = 1_700_000_000_000_000_000
        sync = create_default_sync(now_ns=now_ns, start_enabled=False)
        assert list(sync.ref_time) == [now_ns, now_ns]

    def test_drift_starts_at_identity_ratio(self):
        sync = create_default_sync(now_ns=0)
        assert list(sync.drift_m) == [1_000_000_000, 1_000_000_000]
        assert list(sync.drift_d) == [1_000_000_000, 1_000_000_000]

    def test_auto_sync_fields_start_zeroed_except_drift_and_warmup(self):
        sync = create_default_sync(now_ns=0)
        assert sync.auto_last_raw[0] == 0
        assert sync.auto_init[0] == 0
        assert sync.auto_anchor_raw[0] == 0
        assert sync.auto_drift_m[0] == 1
        assert sync.auto_drift_d[0] == 1
        assert sync.auto_warmup_cnt[0] == AUTO_WARMUP_VAL


class TestPrimeSyncState:
    def test_overwrites_anchors_and_re_enables(self):
        sync = create_default_sync(now_ns=0, start_enabled=False)

        prime_sync_state(sync, phone_ns=123, pc_ns=456)

        assert list(sync.ref_time) == [123, 123]
        assert list(sync.offset) == [456, 456]
        assert sync.enabled[0] == 1
        assert sync.active_idx[0] == 0

    def test_resets_drift_to_identity(self):
        sync = create_default_sync(now_ns=0)
        sync.drift_m[:] = [42, 42]
        sync.drift_d[:] = [7, 7]

        prime_sync_state(sync, phone_ns=1, pc_ns=1)

        assert list(sync.drift_m) == [1_000_000_000, 1_000_000_000]
        assert list(sync.drift_d) == [1_000_000_000, 1_000_000_000]

    def test_mutates_the_arrays_in_place_rather_than_replacing_them(self):
        sync = create_default_sync(now_ns=0)
        ref_time_array = sync.ref_time

        prime_sync_state(sync, phone_ns=99, pc_ns=88)

        assert sync.ref_time is ref_time_array
        assert list(ref_time_array) == [99, 99]
