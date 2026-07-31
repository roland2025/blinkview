# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.dtypes import (
    BYTE,
    ID_TYPE,
    LEN_TYPE,
    LEVEL_TYPE,
    OFFSET_TYPE,
    SEQ_TYPE,
    TS_TYPE,
    UINT32,
    UINT64,
)
from blinkview.core.types.log_batch import LogBundle


def make_log_bundle(
    timestamps,
    devices,
    levels,
    modules,
    sequences,
    messages,
    *,
    rx_timestamps=None,
    pids=None,
    tids=None,
    has_pids=True,
    has_tids=True,
):
    """Builds a minimal LogBundle backing a fixed set of rows.

    rx_timestamps defaults to timestamps; pids/tids default to zeros. has_pids/has_tids
    default to True (the column is present, values default to 0) - pass False explicitly for
    a bundle that should behave as if it has no pids/tids column at all.
    """
    size = len(messages)
    lengths = np.array([len(m) for m in messages], dtype=LEN_TYPE)
    offsets = np.zeros(size, dtype=OFFSET_TYPE)

    cursor = 0
    for i, m in enumerate(messages):
        offsets[i] = cursor
        cursor += len(m.encode("utf-8"))

    buffer = np.zeros(max(cursor, 1), dtype=BYTE)
    cursor = 0
    for m in messages:
        b = m.encode("utf-8")
        if b:
            buffer[cursor : cursor + len(b)] = np.frombuffer(b, dtype=BYTE)
        cursor += len(b)

    if rx_timestamps is None:
        rx_timestamps = timestamps

    return LogBundle(
        timestamps=np.array(timestamps, dtype=TS_TYPE),
        rx_timestamps=np.array(rx_timestamps, dtype=TS_TYPE),
        offsets=offsets,
        lengths=lengths,
        buffer=buffer,
        levels=np.array(levels, dtype=LEVEL_TYPE),
        modules=np.array(modules, dtype=ID_TYPE),
        devices=np.array(devices, dtype=ID_TYPE),
        sequences=np.array(sequences, dtype=SEQ_TYPE),
        pids=np.array(pids if pids is not None else [0] * size, dtype=ID_TYPE),
        tids=np.array(tids if tids is not None else [0] * size, dtype=ID_TYPE),
        ext_u32_1=np.zeros(size, dtype=UINT32),
        ext_u32_2=np.zeros(size, dtype=UINT32),
        ext_u64_1=np.zeros(size, dtype=UINT64),
        size=np.array([size], dtype=np.int64),
        msg_cursor=np.array([cursor], dtype=np.int64),
        capacity=size,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=has_pids,
        has_tids=has_tids,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )
