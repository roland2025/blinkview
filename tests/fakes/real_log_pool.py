# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_log import CircularLogPool


def make_real_log_pool(max_bytes=4 * 1024 * 1024, max_pieces=4, final_buffer_bytes=64 * 1024):
    """Builds a real (array_pool, log_pool) pair - the common preamble shared by tests that
    exercise CircularLogPool/fetch_telemetry_window against a real pool rather than a fake."""
    array_pool = NumpyArrayPool(max_bytes=max_bytes)
    log_pool = CircularLogPool(array_pool, max_pieces=max_pieces, final_buffer_bytes=final_buffer_bytes)
    return array_pool, log_pool
