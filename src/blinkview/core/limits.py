# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

BATCH_QUEUE_MAXLEN = 1_000_000
CENTRAL_STORAGE_MAXLEN = 1_000_000
BATCH_MAXLEN = 2000

CENTRAL_STORAGE_MAX_PIECES = 16
CENTRAL_STORAGE_BUFFER_SIZE_MB = 32

# Cold (disk-backed) tier - see plans/mmap-coldstore.md. Off by default until proven out; a
# non-empty cold_storage_dir is also required (see CentralStorage.apply_config) for it to engage.
CENTRAL_STORAGE_COLD_STORAGE_ENABLED = False
CENTRAL_STORAGE_COLD_MAX_PIECES = 16
