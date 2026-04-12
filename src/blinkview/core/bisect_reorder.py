# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from bisect import bisect_right
from operator import attrgetter

import numpy as np

from blinkview.core.base_reorder import BaseReorder, ReorderFactory
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.log_row import LogRow
from blinkview.core.numpy_batch_manager import PooledLogBatch


@ReorderFactory.register("default")
class Reorder(BaseReorder):
    def __init__(self):
        super().__init__()
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put

    def apply_config(self, config: dict):
        if self.s_ts is None:
            pool = self.shared.array_pool
            # cap = 128_000
            # self._h_ts = pool.acquire(cap, dtype=np.int64)
            # self._h_batch_id = pool.acquire(cap, dtype=np.uint32)
            # self._h_row_idx = pool.acquire(cap, dtype=np.uint32)
            # self._h_lengths = pool.acquire(cap, dtype=np.uint32)

    def run(self):
        pool = self.shared.array_pool
        time_ns = self.shared.time_ns
        delay_s = self.delay / 1000
        delay_ns = self.delay * 1_000_000
        distribute = self.distribute
        get = self.input_queue.get

        batch_out = None

        def flush():
            nonlocal batch_out
            if batch_out is not None and batch_out.size > 0:
                with batch_out:
                    self.distribute(batch_out)
                    pass
            batch_out = None

        stop_is_set = self._stop_event.is_set

        while not stop_is_set():
            now = time_ns()

            # 2. Ingest
            batch_in = get(timeout=delay_s)
            if batch_in is not None:
                print(f"[Reorder] batch_in: {batch_in}")
                # print(f"[REORDER] Received batch of {len(batch_in)} items for reordering")
                # for item in batch_in:
                #     self._insert(item)
