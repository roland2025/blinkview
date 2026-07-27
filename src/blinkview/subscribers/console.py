# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
from rich.text import Text

from blinkview.core.constants import SysCat
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.log_fetch import LogSegmentScanner, LogTextFetcher
from blinkview.core.types.formatting import FormattingConfig
from blinkview.subscribers.subscriber import BaseSubscriber, SubscriberFactory
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from blinkview.utils.utc_offset import get_local_utc_offset_seconds

_EMPTY_SIDEBAR_MASK = np.zeros(0, dtype=np.uint8)


@SubscriberFactory.register("console")
class ConsoleSubscriber(BaseSubscriber):
    """Tails Central Storage's log_pool to a Rich console via LogSegmentScanner/LogTextFetcher
    (core/log_fetch.py) - the same Qt-independent mask-baking/scan/format core
    ui/widgets/log_viewer.py's LogViewerWidget uses, rather than treating pushed PooledLogBatch
    rows as row objects - that contract (msg.level/msg.module.device/msg.message) predates the
    numpy_log rewrite where PooledLogBatch iteration became plain (ts, msg_bytes, ...) tuples of
    raw ids.

    Still subscribes to STORAGE/REORDER for lifecycle purposes (registry.build_subscriber wires
    subscribers through the same pub/sub topology as everything else), but the pushed batches
    are only used as a "there might be new data" wake-up signal and immediately released -
    the actual rows are pulled straight from central.log_pool on every tick, exactly like the
    GUI log viewer does.

    Has no module-filter-sidebar/kv/text-filter concept, unlike LogViewerWidget - LogFilter is
    constructed with only a level threshold, and get_sidebar_filter/get_show_hidden are passed
    trivial always-permissive callables so ensure_effective_mask() degenerates to a flat
    level-only mask, matching this class's original hand-rolled
    np.full(mod_count, log_level) one-liner (see tests/test_log_fetch.py's degenerate-mask test).
    """

    def __init__(self, console):
        print("[Console] init")
        super().__init__()

        self.sources = [SysCat.STORAGE, SysCat.REORDER]

        self.console = console

        self.streaming = True

        self.log_level = LogLevel.ALL

        # Constructed once id_registry is available, at the top of run() - LogFilter needs a
        # real IDRegistry to resolve_device/resolve_module against, which self.shared only
        # provides once the subscriber thread starts. set_level() (called from another thread,
        # e.g. the CLI's keypress handler) keeps self.log_filter in sync once it exists so a
        # level change mid-run takes effect on the very next tick, same as before.
        self.log_filter = None
        self._scanner = None

        self.latest_seq_seen = SEQ_NONE

    def set_level(self, level: LogLevel):
        self.log_level = level
        if self.log_filter is not None:
            self.log_filter.set_level(level.name_conf)
            self._scanner.invalidate_mask()

    def run(self):
        registry = self.shared.registry
        pool = registry.central.log_pool
        reg = self.shared.id_registry
        array_pool = self.shared.array_pool

        self.log_filter = LogFilter(reg, log_level=self.log_level.name_conf)
        self._scanner = LogSegmentScanner(
            reg,
            lambda: pool,
            self.log_filter,
            get_sidebar_filter=lambda: (False, _EMPTY_SIDEBAR_MASK),
            get_show_hidden=lambda: True,
        )
        fetcher = LogTextFetcher(self._scanner, array_pool, tables_provider=reg.bundle)

        format_cfg = FormattingConfig(
            show_ts=True, show_dev=True, show_lvl=True, show_mod=True, ts_precision=3, show_date=False
        )
        tz_offset_sec = get_local_utc_offset_seconds()

        fetcher.latest_seq_seen = pool.latest_sequence()

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

            # Unbounded live tail: match the original behavior of capping only at the pool's own
            # indices-buffer capacity, not an artificial viewport-style row count.
            result = fetcher.fetch_live_tail(
                max_rows=pool.segment_capacity, format_cfg=format_cfg, tz_offset_sec=tz_offset_sec
            )

            if result.text:
                c_print(Text(result.text), soft_wrap=True, end="")

            self.latest_seq_seen = fetcher.latest_seq_seen
