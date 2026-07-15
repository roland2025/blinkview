# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
from rich.text import Text

from ..core import dtypes
from ..core.constants import SysCat
from ..core.dtypes import SEQ_NONE
from ..core.types.formatting import FormattingConfig
from ..ops.formatting import nb_segment_estimate_out_size, nb_segment_format
from ..ops.segments import segment_filter_reversed
from ..utils.log_level import LogLevel
from ..utils.utc_offset import get_local_utc_offset_seconds
from .subscriber import BaseSubscriber, SubscriberFactory


@SubscriberFactory.register("console")
class ConsoleSubscriber(BaseSubscriber):
    """Tails Central Storage's log_pool to a Rich console, using the same pull/snapshot query
    pattern as ui/widgets/log_viewer.py (get_reversed_snapshot + segment_filter_reversed +
    nb_segment_format), rather than treating pushed PooledLogBatch rows as row objects - that
    contract (msg.level/msg.module.device/msg.message) predates the numpy_log rewrite where
    PooledLogBatch iteration became plain (ts, msg_bytes, ...) tuples of raw ids.

    Still subscribes to STORAGE/REORDER for lifecycle purposes (registry.build_subscriber wires
    subscribers through the same pub/sub topology as everything else), but the pushed batches
    are only used as a "there might be new data" wake-up signal and immediately released -
    the actual rows are pulled straight from central.log_pool on every tick, exactly like the
    GUI log viewer does.
    """

    def __init__(self, console):
        print("[Console] init")
        super().__init__()

        self.sources = [SysCat.STORAGE, SysCat.REORDER]

        self.console = console

        self.streaming = True

        self.log_level = LogLevel.ALL

        self.latest_seq_seen = SEQ_NONE

    def set_level(self, level: LogLevel):
        self.log_level = level

    def run(self):
        registry = self.shared.registry
        pool = registry.central.log_pool
        reg = self.shared.id_registry
        array_pool = self.shared.array_pool

        format_cfg = FormattingConfig(
            show_ts=True, show_dev=True, show_lvl=True, show_mod=True, ts_precision=3, show_date=False
        )
        tz_offset_sec = get_local_utc_offset_seconds()

        self.latest_seq_seen = pool.latest_sequence()

        drain = self.input_queue.get
        c_print = self.console.print
        stop_is_set = self._stop_event.is_set

        print(f"[Console] started with log level: {self.log_level}")

        while not stop_is_set():
            pushed = drain(0.1)
            if pushed is not None:
                pushed.release()

            if not self.streaming:
                continue

            mod_count = reg.module_count()
            if mod_count == 0:
                continue

            effective_mask = np.full(mod_count, dtypes.LEVEL_TYPE(self.log_level.value), dtype=dtypes.LEVEL_TYPE)
            reg_bundle = reg.bundle()

            string_batches = []
            with pool.get_reversed_snapshot() as segments, pool.acquire_indices_buffer() as indices:
                for segment in segments:
                    if segment.size == 0 or segment.last_sequence_id <= self.latest_seq_seen:
                        break

                    match_count = segment_filter_reversed(
                        segment.bundle,
                        effective_mask=effective_mask,
                        out_indices=indices.array,
                        max_matches=len(indices.array),
                        start_seq=self.latest_seq_seen,
                    )

                    if match_count > 0:
                        req_bytes = nb_segment_estimate_out_size(
                            indices.array, match_count, segment.bundle, reg_bundle, format_cfg
                        )
                        with array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                            bytes_written = nb_segment_format(
                                handle.array,
                                indices.array,
                                match_count,
                                segment.bundle,
                                reg_bundle,
                                format_cfg,
                                tz_offset_sec,
                            )
                            string_batches.append(
                                handle.array[:bytes_written].tobytes().decode("utf-8", errors="replace")
                            )

            self.latest_seq_seen = pool.latest_sequence()

            if string_batches:
                string_batches.reverse()
                c_print(Text("".join(string_batches)), soft_wrap=True, end="")
