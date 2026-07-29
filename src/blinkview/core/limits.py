# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

BATCH_QUEUE_MAXLEN = 1_000_000
CENTRAL_STORAGE_MAXLEN = 1_000_000
BATCH_MAXLEN = 2000

CENTRAL_STORAGE_MAX_PIECES = 2
CENTRAL_STORAGE_BUFFER_SIZE_MB = 128

# Cold (disk-backed) tier - see plans/mmap-coldstore.md. On by default: 32 pieces x
# CENTRAL_STORAGE_BUFFER_SIZE_MB (128MB) = ~4GB of extra on-disk scrollback per session, written to
# this session's own log folder (see CentralStorage._resolve_cold_storage_dir). Bigger, fewer
# pieces than a naive split (vs. e.g. 128 x 32MB) deliberately keeps the same total budget while
# cutting the per-fetch segment-count-scaling overhead (SegmentSnapshot retain/release, per-segment
# skip checks - see plans/fetch-telemetry-window-cold-segment-perf.md) by roughly the same factor.
CENTRAL_STORAGE_COLD_STORAGE_ENABLED = True
CENTRAL_STORAGE_COLD_MAX_PIECES = 32
