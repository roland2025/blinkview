# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from calendar import timegm
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.log_level import LogLevel

# Mirrors the fixed grammar written by ops/formatting.py's nb_format_log_row_batch:
#   YYYY-MM-DDTHH:MM:SS.uuuuuuZ <LEVEL> <DEVICE> <MODULE>: <MESSAGE>\n
# Level/device/module are single tokens (device/module names are restricted to
# [a-z0-9_.]+ by DeviceIdentity._VALID_NAME_REGEX, so they never contain whitespace).
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})Z "
    r"(?P<level>\S+) (?P<device>\S+) (?P<module>\S+): (?P<message>.*)$"
)

# id_registry.py registers each LogLevel by its short `.name` (e.g. "I", "W"), not `.name_conf".
_LEVEL_BY_CHAR = {level.name: level.value for level in LogLevel.LIST}


def _parse_ts_ns(ts_text: str) -> int:
    """Reverses ISO8601TimestampFormatter-style output: 'YYYY-MM-DDTHH:MM:SS.uuuuuu' (UTC)."""
    dt = datetime.strptime(ts_text[:19], "%Y-%m-%dT%H:%M:%S")
    us = int(ts_text[20:])
    return timegm(dt.timetuple()) * 1_000_000_000 + us * 1000


class UnifiedLogReplay(BaseDaemon):
    """One-shot reader that loads a previously-written unified log (FileLogger/log_row
    output) back into Central Storage. Not a live source/parser - constructed and
    subscribed directly to registry.central, bypassing Reorder since a single unified
    log file is already chronologically ordered.
    """

    MAX_BATCH_ROWS = 4096
    BUFFER_BYTES = MAX_BATCH_ROWS * 256

    def __init__(self, log_parts: Iterable[Path]):
        super().__init__()
        self.log_parts: List[Path] = list(log_parts)
        self.enabled = True

    def run(self):
        id_registry = self.shared.id_registry
        pool_create = self.shared.array_pool.create
        stop_is_set = self._stop_event.is_set

        def batch_acquire():
            return pool_create(
                PooledLogBatch,
                self.MAX_BATCH_ROWS,
                self.BUFFER_BYTES,
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        batch = batch_acquire()

        try:
            for part in self.log_parts:
                if stop_is_set():
                    break
                with open(part, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        if stop_is_set():
                            break

                        line = line.rstrip("\n")
                        if not line:
                            continue

                        m = _LINE_RE.match(line)
                        if m is None:
                            if self.logger:
                                self.logger.warn(f"UnifiedLogReplay: unparseable line skipped: {line[:80]!r}")
                            continue

                        ts_ns = _parse_ts_ns(m.group("ts"))
                        level = _LEVEL_BY_CHAR.get(m.group("level"), LogLevel.INFO.value)
                        device_identity = id_registry.get_device(m.group("device"))
                        module_identity = device_identity.get_module(m.group("module"))
                        message = m.group("message").encode("utf-8")

                        if not batch.insert_any(
                            ts_ns,
                            ts_ns,
                            message,
                            level=level,
                            module=module_identity.id,
                            device=device_identity.id,
                        ):
                            with batch:
                                self.distribute(batch)
                            batch = batch_acquire()
                            batch.insert_any(
                                ts_ns,
                                ts_ns,
                                message,
                                level=level,
                                module=module_identity.id,
                                device=device_identity.id,
                            )

            if batch.size > 0:
                with batch:
                    self.distribute(batch)
                batch = None
            else:
                batch.release()
                batch = None
        except Exception as e:
            if self.logger:
                self.logger.exception("UnifiedLogReplay: error during replay", e)
        finally:
            if batch is not None:
                batch.release()

            if self.logger:
                self.logger.info(f"UnifiedLogReplay: finished replaying {len(self.log_parts)} part(s).")
