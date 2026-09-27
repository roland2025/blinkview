# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core import dtypes


class CodecID:
    NONE = 0
    NEWLINE = 10
    COBS = 20
    SLIP = 30
    ADB_LONG = 40

    # 99 is reserved for custom/plugin decoders
    PLUGIN = 99


STATE_COMPLETE = 0
STATE_INCOMPLETE = 1
STATE_ERROR = 2


class ParserConfig(NamedTuple):
    level_default: int = 0
    level_error: int = 0
    module_unknown: int = 0
    module_log: int = 0
    device_id: int = 0
    report_error: bool = False
    filter_squash_spaces: bool = False


EmptyParserConfig = ParserConfig()


class InputParams(NamedTuple):
    ts_in: int  # Timestamp for this batch
    split_char: int  # Delimiter (e.g., ord('\n'))
    def_lvl: int  # Default Level
    def_mod: int  # Default Module ID
    def_dev: int  # Default Device ID


class LogOutput(NamedTuple):
    ts: np.ndarray  # int64
    off: np.ndarray  # uint32
    lengths: np.ndarray  # uint32
    buf: np.ndarray  # uint8
    # Optional columns (Pass empty arrays if not used)
    lvl: np.ndarray  # uint8
    mod: np.ndarray  # uint16
    dev: np.ndarray  # uint16
    seq: np.ndarray  # uint64
    # Status flags
    has_lvl: bool
    has_mod: bool
    has_dev: bool
    has_seq: bool


U8 = np.uint8
I64 = dtypes.INT64

SYNC_ENABLED_OFF = np.zeros(1, dtype=U8)
SYNC_ACTIVE_UNUSED = np.zeros(1, dtype=I64)
SYNC_OFFSET_ZERO = np.zeros(2, dtype=I64)
SYNC_REF_ZERO = np.zeros(2, dtype=I64)
SYNC_RATIO_ONE = np.ones(2, dtype=I64)

# Auto-Sync Constants
AUTO_BASE_ZERO = np.zeros(1, dtype=I64)
AUTO_LAST_ZERO = np.zeros(1, dtype=I64)
AUTO_INIT_OFF = np.zeros(1, dtype=U8)
AUTO_DRIFT_ONE = np.ones(1, dtype=I64)

AUTO_WARMUP_VAL = 512  # Number of logs to stay in "fast-sync" mode


class SyncState(NamedTuple):
    enabled: np.ndarray = SYNC_ENABLED_OFF
    active_idx: np.ndarray = SYNC_ACTIVE_UNUSED
    offset: np.ndarray = SYNC_OFFSET_ZERO
    ref_time: np.ndarray = SYNC_REF_ZERO
    drift_m: np.ndarray = SYNC_RATIO_ONE
    drift_d: np.ndarray = SYNC_RATIO_ONE

    # --- Auto-Sync Fallback Fields ---
    auto_last_raw: np.ndarray = AUTO_BASE_ZERO
    auto_init: np.ndarray = AUTO_INIT_OFF

    # Anchor points used to calculate projection
    auto_anchor_raw: np.ndarray = AUTO_BASE_ZERO
    auto_anchor_rx: np.ndarray = AUTO_BASE_ZERO

    # Windowed trackers to find the "fastest" packet (lowest delay)
    auto_window_raw: np.ndarray = AUTO_BASE_ZERO
    auto_window_rx: np.ndarray = AUTO_BASE_ZERO
    auto_window_min_offset: np.ndarray = AUTO_BASE_ZERO

    # Calculated k-value (drift ratio)
    auto_drift_m: np.ndarray = AUTO_DRIFT_ONE
    auto_drift_d: np.ndarray = AUTO_DRIFT_ONE

    auto_warmup_cnt: np.ndarray = AUTO_BASE_ZERO


UnusedSyncState = SyncState()


def create_default_sync(now_ns: int, start_enabled: bool = False):
    """
    Factory for a device that MIGHT sync later.
    Anchors the initial state to the provided PC timestamp.
    """
    identity_drift = 1_000_000_000

    # If enabled immediately, offset must match ref_time to prevent
    # the '1970 epoch bug'.
    initial_offset = now_ns if start_enabled else 0

    return SyncState(
        enabled=np.array([1 if start_enabled else 0], dtype=U8),
        active_idx=np.array([0], dtype=I64),
        # PC anchor time
        offset=np.array([initial_offset, initial_offset], dtype=I64),
        # Phone anchor time
        ref_time=np.array([now_ns, now_ns], dtype=I64),
        drift_m=np.array([identity_drift, identity_drift], dtype=I64),
        drift_d=np.array([identity_drift, identity_drift], dtype=I64),
        # --- Auto-Sync Fields ---
        auto_last_raw=np.zeros(1, dtype=I64),
        auto_init=np.zeros(1, dtype=U8),
        auto_anchor_raw=np.zeros(1, dtype=I64),
        auto_anchor_rx=np.zeros(1, dtype=I64),
        auto_window_raw=np.zeros(1, dtype=I64),
        auto_window_rx=np.zeros(1, dtype=I64),
        auto_window_min_offset=np.zeros(1, dtype=I64),
        auto_drift_m=np.ones(1, dtype=I64),
        auto_drift_d=np.ones(1, dtype=I64),
        auto_warmup_cnt=np.array([AUTO_WARMUP_VAL], dtype=I64),
    )


def prime_sync_state(sync: SyncState, phone_ns: int, pc_ns: int):
    """
    Forcefully anchors the sync state to a coarse baseline.
    Ensures math is identity (1:1) until the high-precision syncer takes over.
    """
    # 1. Set Anchors (NumPy handles int -> int64 casting here)
    sync.ref_time[:] = [phone_ns, phone_ns]
    sync.offset[:] = [pc_ns, pc_ns]

    # 2. Reset Drift to Identity (1.0 ratio)
    # Using 1,000,000,000 matches the 'ppb_scale' in the Numba kernel
    identity_val = 1_000_000_000
    sync.drift_m[:] = [identity_val, identity_val]
    sync.drift_d[:] = [identity_val, identity_val]

    # 3. Reset Active Index
    sync.active_idx[0] = 0

    # 4. Enable the Bridge
    # Setting uint8 array to 1 (True)
    sync.enabled[0] = 1


TS_PRECISION_S = 0  # Seconds
TS_PRECISION_MS = 1  # Milliseconds
TS_PRECISION_US = 2  # Microseconds
TS_PRECISION_NS = 3  # Nanoseconds
