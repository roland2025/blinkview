# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

# A BatchQueue is bounded by rows and by the memory its batches hold (allocated, not used). The
# byte bound is what keeps memory in check: a row bound alone let ~120 nearly empty 292 MB batches
# (~35 GB) queue up under 1 M rows. So the row bound is set for throughput, not memory: 16 M rows is
# ~1.3 s at 12 M msg/s (1 M rows was less than the reorderer's 100 ms delay window at that rate).
BATCH_QUEUE_MAXLEN = 16_000_000
BATCH_QUEUE_MAX_BYTES = 1024 * 1024 * 1024  # ~64 full 16 MB batches
# The reorderer's input queue takes every source at once: ~1.5 s at 12 M msg/s of 64-byte lines
# (dense batches measured at ~110 bytes/row allocated).
REORDER_QUEUE_MAX_BYTES = 2 * 1024 * 1024 * 1024
# Rows the reorderer merges in one pass. A backlog is merged in slices of this size, so its output
# batches stay pool-sized and the merge's working set stays in cache: unbounded passes took 4-20 M
# rows and 0.4-1.6 GB batches under a backlog. Measured with a 2 M-row backlog, 2^17 was as fast
# as or faster than 2^18 and 2^19 for 12- and 64-byte lines (64 B: 40 vs 24 M rows/s unbounded).
REORDER_MAX_ROWS_PER_PASS = 1 << 17
CENTRAL_STORAGE_MAXLEN = 1_000_000
BATCH_MAXLEN = 2000

# ThroughputAutoTuner's ceiling on the batches it sizes (rate x window x safety has no bound of its
# own: a fast loopback source asked for 64-256 MB batches). 16 MB covers ~107 MB/s at the readers'
# 100 ms delay; past that a full batch is handed on early, which is cheap. Both are powers of two,
# so the pool's power-of-two slabs never round past them, and they stay well under the pool's
# 64 MB max_bytes so these batches are reused instead of reallocated.
TUNER_MAX_BUFFER_BYTES = 16 * 1024 * 1024
TUNER_MAX_CAPACITY = 1 << 18

CENTRAL_STORAGE_MAX_PIECES = 2
CENTRAL_STORAGE_BUFFER_SIZE_MB = 128

# Cold (disk-backed) tier - see plans/mmap-coldstore.md. On by default: 32 pieces x
# CENTRAL_STORAGE_BUFFER_SIZE_MB (128MB) = ~4GB of extra on-disk scrollback per session, written to
# this session's own log folder (see CentralStorage._resolve_cold_storage_dir). Bigger, fewer
# pieces than a naive split (vs. e.g. 128 x 32MB) deliberately keeps the same total budget while
# cutting the per-fetch segment-count-scaling overhead (SegmentSnapshot retain/release, per-segment
# skip checks - see plans/fetch-telemetry-window-cold-segment-perf.md) by roughly the same factor.
CENTRAL_STORAGE_COLD_STORAGE_ENABLED = True
CENTRAL_STORAGE_COLD_MAX_PIECES = 128

# HotTierMemoryGovernor - auto-sizing the hot tier off system free memory instead of a static
# max_pieces knob. See plans/auto-hot-cold-memory-management.md. Opt-in (off by default) and only
# takes effect when cold storage is enabled (see CentralStorage._apply_memory_governor_config) -
# otherwise "shrink hot tier under memory pressure" would silently become "delete scrollback".
CENTRAL_STORAGE_AUTO_MEMORY_MANAGEMENT_ENABLED = False
CENTRAL_STORAGE_MIN_HOT_PIECES = CENTRAL_STORAGE_MAX_PIECES
CENTRAL_STORAGE_MAX_HOT_PIECES = 0  # 0 = unbounded except by memory pressure itself
CENTRAL_STORAGE_TARGET_FREE_MEMORY_MB = 4096
CENTRAL_STORAGE_MEMORY_POLL_INTERVAL_SEC = 3.0

# Caps a single HotTierMemoryGovernor poll tick's shrink step, bounding worst-case
# CircularLogPool._lock hold time predictably regardless of how large a sudden memory spike is - a
# persistent spike beyond this many segments' worth just takes a few extra poll ticks to fully
# react to. Not a benchmarked value, a starting guess (see plan doc's "Rate limiting" section).
MAX_SEGMENTS_EVICTED_PER_TICK = 4
