# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.codec_adb_long import nb_parse_adb_pid_tid


def make_out_bundle(capacity):
    """Minimal LogBundle with has_pids/has_tids=True, for testing the pid/tid write-out."""
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
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=True,
        has_tids=True,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def test_nb_parse_adb_pid_tid_writes_parsed_pid_and_tid():
    msg = "2680:2701 I/Tag"
    buf = np.frombuffer(msg.encode("utf-8"), dtype=dtypes.BYTE)
    out_b = make_out_bundle(1)

    next_cursor = nb_parse_adb_pid_tid(buf, 0, len(msg), out_b, 0, 0, 0)

    assert int(out_b.pids[0]) == 2680
    assert int(out_b.tids[0]) == 2701
    # Cursor should land at the start of the log level ("I/Tag").
    assert buf[next_cursor : next_cursor + 1].tobytes() == b"I"


def test_nb_parse_adb_pid_tid_handles_no_space_after_colon():
    msg = "100:200 W/Tag"
    buf = np.frombuffer(msg.encode("utf-8"), dtype=dtypes.BYTE)
    out_b = make_out_bundle(1)

    nb_parse_adb_pid_tid(buf, 0, len(msg), out_b, 0, 0, 0)

    assert int(out_b.pids[0]) == 100
    assert int(out_b.tids[0]) == 200


def test_nb_parse_adb_pid_tid_writes_at_out_idx():
    msg = "5:6 I/Tag"
    buf = np.frombuffer(msg.encode("utf-8"), dtype=dtypes.BYTE)
    out_b = make_out_bundle(3)

    nb_parse_adb_pid_tid(buf, 0, len(msg), out_b, 2, 0, 0)

    assert int(out_b.pids[2]) == 5
    assert int(out_b.tids[2]) == 6
    assert int(out_b.pids[0]) == 0
    assert int(out_b.pids[1]) == 0
