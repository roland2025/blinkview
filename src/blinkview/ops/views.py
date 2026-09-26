# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Meminfo-free array views for the hot parser kernels.

Every refcounted array argument crossing an (inlined) call boundary costs an atomic incref/decref pair.
A ``numba.carray`` over a raw pointer has a null meminfo, so the refcount traffic disappears. The
``nb_*_views`` helpers rebuild the parser state/config/output NamedTuples with every array field replaced by
such a view; scalars are copied as is, so step code keeps using the same dotted names.

Rules: views never escape the kernel call that created them (they do not keep memory alive), they assume
1-D C-contiguous arrays, and they must not be shared across threads.
"""

from numba import carray, types
from numba.extending import intrinsic

from blinkview.core.id_registry.types import StringTableParams
from blinkview.core.numba_config import NUMBA_DISABLE, app_njit
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.modules import DynamicWidthConfig, ModuleTrackerState
from blinkview.core.types.parsing import SyncState, TimeParserState, UnifiedParserConfig, UnifiedParserState

if NUMBA_DISABLE:

    def nb_view(a):
        return a

else:

    @intrinsic
    def nb_addr_to_ptr(typingctx, addr, ref):
        sig = types.CPointer(ref.dtype)(addr, ref)

        def codegen(context, builder, sig, args):
            return builder.inttoptr(args[0], context.get_value_type(sig.return_type))

        return sig, codegen

    @app_njit(inline="always")
    def nb_view(a):
        return carray(nb_addr_to_ptr(a.ctypes.data, a), a.shape)


@app_njit(inline="always")
def nb_sync_views(s):
    return SyncState(
        nb_view(s.enabled),
        nb_view(s.active_idx),
        nb_view(s.offset),
        nb_view(s.ref_time),
        nb_view(s.drift_m),
        nb_view(s.drift_d),
        nb_view(s.auto_last_raw),
        nb_view(s.auto_init),
        nb_view(s.auto_anchor_raw),
        nb_view(s.auto_anchor_rx),
        nb_view(s.auto_window_raw),
        nb_view(s.auto_window_rx),
        nb_view(s.auto_window_min_offset),
        nb_view(s.auto_drift_m),
        nb_view(s.auto_drift_d),
        nb_view(s.auto_warmup_cnt),
    )


@app_njit(inline="always")
def nb_state_views(st):
    m = st.modules
    t = st.timestamp
    return UnifiedParserState(
        ModuleTrackerState(
            nb_view(m.count),
            nb_view(m.bytes_cursor),
            nb_view(m.starts),
            nb_view(m.lengths),
            nb_view(m.hashes),
            nb_view(m.name_bytes),
        ),
        TimeParserState(nb_view(t.utc_offset), nb_sync_views(t.sync)),
    )


@app_njit(inline="always")
def nb_config_views(c):
    s = c.string_table
    d = c.module_config
    return UnifiedParserConfig(
        c.parser_id,
        c.parser_config,
        StringTableParams(
            nb_view(s.buffer),
            nb_view(s.offsets),
            nb_view(s.lens),
            nb_view(s.hashes),
            nb_view(s.values),
            s.count,
            nb_view(s.hash_index),
        ),
        DynamicWidthConfig(
            d.max_length,
            d.max_depth,
            d.enable_brackets,
            d.enable_dot_separator,
            nb_view(d.prefix_bytes),
            d.prefix_match,
            d.prefix_remove,
        ),
        c.timestamp_precision,
        c.timestamp_unix,
        c.syslog_year,
    )


@app_njit(inline="always")
def nb_log_bundle_views(o):
    return LogBundle(
        nb_view(o.timestamps),
        nb_view(o.rx_timestamps),
        nb_view(o.offsets),
        nb_view(o.lengths),
        nb_view(o.buffer),
        nb_view(o.levels),
        nb_view(o.modules),
        nb_view(o.devices),
        nb_view(o.sequences),
        nb_view(o.pids),
        nb_view(o.tids),
        nb_view(o.ext_u32_1),
        nb_view(o.ext_u32_2),
        nb_view(o.ext_u64_1),
        nb_view(o.size),
        nb_view(o.msg_cursor),
        o.capacity,
        o.has_levels,
        o.has_modules,
        o.has_devices,
        o.has_sequences,
        o.has_pids,
        o.has_tids,
        o.has_ext_u32_1,
        o.has_ext_u32_2,
        o.has_ext_u64_1,
    )
