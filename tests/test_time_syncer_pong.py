# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The pong format shared by the serial (bsync) and logcat (blinksync.pong) time syncers:
"r s=<seq> m=<mono> b=<boot> w=<wall>" (wiki:specs/blinksync-time-sync.md).

Skipped under BLINKVIEW_DISABLE_NUMBA=1: nb_find_and_parse_int's digit arithmetic on uint8 buffer
elements overflows in plain Python (see tests/test_ops_zephyr_timestamp.py), which the JIT path
never does."""

import os

import pytest

from blinkview.core import dtypes
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ops.blinksync import (
    KEY_BOOT,
    KEY_MONO,
    KEY_SEQ,
    PONG_PREFIX,
    nb_find_and_parse_next_pong,
    nb_parse_pong_payload,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("BLINKVIEW_DISABLE_NUMBA") == "1", reason="uint8 digit arithmetic overflows without the JIT"
)

PONG_MODULE = 7
OTHER_MODULE = 3


@pytest.fixture
def make_batch(array_pool):
    batches = []

    def make(*rows):
        """rows: (module, message, rx_ts)"""
        batch = array_pool.create(PooledLogBatch, 16, 1024, has_modules=True)
        for module, message, rx_ts in rows:
            batch.insert(rx_ts, rx_ts, message, module=module)
        batches.append(batch)
        return batch.bundle

    yield make
    for batch in batches:
        batch.release()


def parse(bundle, index):
    return nb_parse_pong_payload(bundle, index, PONG_PREFIX, KEY_SEQ, KEY_MONO, KEY_BOOT)


def find_all(bundle):
    found_pongs = []
    cursor = 0
    while True:
        found, is_pong, cursor, seq, mono, boot, rx_ts = nb_find_and_parse_next_pong(
            bundle, dtypes.ID_TYPE(PONG_MODULE), dtypes.SEQ_TYPE(cursor), PONG_PREFIX, KEY_SEQ, KEY_MONO, KEY_BOOT
        )
        if not found:
            return found_pongs
        found_pongs.append((is_pong, seq, mono, boot, rx_ts))


class TestPongPayload:
    def test_logcat_pong_with_every_clock(self, make_batch):
        bundle = make_batch((PONG_MODULE, b"r s=7 m=123456789012 b=223456789012 w=1764000000000000000", 1))
        assert parse(bundle, 0) == (True, 7, 123456789012, 223456789012)

    def test_serial_pong_without_wall_clock(self, make_batch):
        bundle = make_batch((PONG_MODULE, b"r s=3 m=1000 b=2000", 1))
        assert parse(bundle, 0) == (True, 3, 1000, 2000)

    def test_omitted_clock_reads_as_zero(self, make_batch):
        bundle = make_batch((PONG_MODULE, b"r s=4 b=2000", 1))
        assert parse(bundle, 0) == (True, 4, 0, 2000)

    def test_old_full_name_logcat_pong_is_not_a_pong(self, make_batch):
        bundle = make_batch((PONG_MODULE, b"seq=7 tx=1 mono=2 boot=3 wall=4", 1))
        assert parse(bundle, 0) == (False, -1, 0, 0)

    def test_heartbeat_is_not_a_pong(self, make_batch):
        bundle = make_batch((PONG_MODULE, b"m=1 b=2 w=3", 1))
        assert parse(bundle, 0)[0] is False


class TestFindNextPong:
    def test_walks_only_the_pong_module_rows_in_order(self, make_batch):
        bundle = make_batch(
            (OTHER_MODULE, b"r s=99 m=1 b=1", 10),  # right format, wrong module
            (PONG_MODULE, b"status initialized_idle", 20),
            (PONG_MODULE, b"r s=1 m=100 b=200 w=300", 30),
            (OTHER_MODULE, b"hello", 40),
            (PONG_MODULE, b"r s=2 m=101 b=201", 50),
        )
        assert find_all(bundle) == [
            (False, -1, 0, 0, 20),
            (True, 1, 100, 200, 30),
            (True, 2, 101, 201, 50),
        ]

    def test_no_pong_rows(self, make_batch):
        bundle = make_batch((OTHER_MODULE, b"hello", 1))
        assert find_all(bundle) == []
