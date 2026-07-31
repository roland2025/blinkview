# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import mmap
import os
from contextlib import ExitStack
from pathlib import Path
from typing import Iterable, List

import numpy as np

from blinkview.core import dtypes
from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.modules import MODULE_ID_FULL, MODULE_TEMP_ID_BASE, ModuleTrackerState
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.id_resolution import nb_resolve_names_batch, nb_resolve_scoped_names_batch
from blinkview.ops.unified_log_scan import nb_push_unified_log_rows, nb_scan_unified_log_lines
from blinkview.storage.log_file_archive import ARCHIVE_SUFFIX, decompress_log_part_to_buffer

# Fixed grammar written by ops/formatting.py's nb_format_log_row_batch:
#   YYYY-MM-DDTHH:MM:SS.uuuuuuZ <LEVEL> <DEVICE> <MODULE>: <MESSAGE>\n
# Level/device/module are single tokens (device/module names are restricted to
# [a-z0-9_.]+ by DeviceIdentity._VALID_NAME_REGEX, so they never contain whitespace).
# Parsed entirely by ops/unified_log_scan.py's Numba kernels (nb_scan_unified_log_lines,
# nb_push_unified_log_rows) - see that module for the byte-level grammar scan and
# ops/timestamps.py's nb_parse_unified_log_ts_ns for the timestamp math.


class UnifiedLogReplay(BaseDaemon):
    """One-shot reader that loads a previously-written unified log (FileLogger/log_row
    output) back into Central Storage. Not a live source/parser - constructed and
    subscribed directly to registry.central, bypassing Reorder since a single unified
    log file is already chronologically ordered.
    """

    # 10MiB/256 bytes-per-row -> ~30 batches for the 1.2M-row demo session instead of ~299 -
    # row capacity (not the byte buffer) was the limiter at the old 4096, since real messages
    # average well under 256 bytes.
    MAX_BATCH_ROWS = 40960
    BUFFER_BYTES = MAX_BATCH_ROWS * 256
    MAX_MALFORMED = 256

    def __init__(self, log_parts: Iterable[Path]):
        super().__init__()
        self.log_parts: List[Path] = list(log_parts)
        self.enabled = True

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Compiles nb_scan_unified_log_lines, nb_push_unified_log_rows (and, transitively,
        nb_parse_unified_log_ts_ns/nb_bundle_push_len), and the id-resolution kernels
        (nb_resolve_names_batch/nb_resolve_scoped_names_batch, transitively nb_resolve_scoped_id
        and ops/discovery.py's nb_resolve_module_id) against real dummy data - a small in-memory
        buffer built from the exact grammar these kernels expect, pushed into a real pooled
        batch, mirroring what run() does per part/scan-batch."""
        sample = ("2026-01-01T00:00:00.000000Z I nrf52 log: hello world\nnot a matching line at all\n").encode("utf-8")
        buf = np.frombuffer(sample, dtype=np.uint8)

        max_rows = 8
        max_malformed = 8
        ts_ns = np.empty(max_rows, dtype=np.int64)
        level = np.empty(max_rows, dtype=np.int64)
        dev_off = np.empty(max_rows, dtype=np.int64)
        dev_len = np.empty(max_rows, dtype=np.int64)
        mod_off = np.empty(max_rows, dtype=np.int64)
        mod_len = np.empty(max_rows, dtype=np.int64)
        msg_off = np.empty(max_rows, dtype=np.int64)
        msg_len = np.empty(max_rows, dtype=np.int64)
        malformed_off = np.empty(max_malformed, dtype=np.int64)
        malformed_len = np.empty(max_malformed, dtype=np.int64)

        rows_found, _cursor, _malformed_found, _overflow = nb_scan_unified_log_lines(
            buf,
            0,
            max_rows,
            ts_ns,
            level,
            dev_off,
            dev_len,
            mod_off,
            mod_len,
            msg_off,
            msg_len,
            malformed_off,
            malformed_len,
            max_malformed,
        )

        device = helper.registry.get_device("nrf52")
        module = device.get_module("log")

        device_table = IndexedStringTable(initial_capacity=4, buffer_size_bytes=64, use_hashes=True)
        device_table.register_name(device.id, "nrf52")
        module_table = IndexedStringTable(
            initial_capacity=4, buffer_size_bytes=64, use_hashes=True, values_dtype=dtypes.ID_TYPE
        )
        module_table.register_name(module.id, "log", value=device.id)
        tracker = ModuleTrackerState(
            count=np.zeros(1, dtype=dtypes.ID_TYPE),
            bytes_cursor=np.zeros(1, dtype=dtypes.OFFSET_TYPE),
            starts=np.zeros(4, dtype=dtypes.OFFSET_TYPE),
            lengths=np.zeros(4, dtype=dtypes.LEN_TYPE),
            hashes=np.zeros(4, dtype=dtypes.HASH_TYPE),
            name_bytes=buf,
        )

        device_id = np.empty(max_rows, dtype=np.int64)
        module_id = np.empty(max_rows, dtype=np.int64)
        nb_resolve_names_batch(buf, dev_off, dev_len, rows_found, device_table.bundle(), tracker, device_id)
        tracker.count[0] = 0
        nb_resolve_scoped_names_batch(
            buf, mod_off, mod_len, device_id, rows_found, module_table.bundle(), tracker, module_id
        )

        batch = helper.array_pool.create(
            PooledLogBatch, max_rows, max_rows * 256, has_levels=True, has_modules=True, has_devices=True
        )
        try:
            nb_push_unified_log_rows(
                batch.bundle, buf, ts_ns, level, device_id, module_id, msg_off, msg_len, rows_found
            )
        finally:
            batch.release()

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

        # Preallocated once, reused across every scan call (all parts, all batches).
        max_rows = self.MAX_BATCH_ROWS
        ts_ns = np.empty(max_rows, dtype=np.int64)
        level = np.empty(max_rows, dtype=np.int64)
        dev_off = np.empty(max_rows, dtype=np.int64)
        dev_len = np.empty(max_rows, dtype=np.int64)
        mod_off = np.empty(max_rows, dtype=np.int64)
        mod_len = np.empty(max_rows, dtype=np.int64)
        msg_off = np.empty(max_rows, dtype=np.int64)
        msg_len = np.empty(max_rows, dtype=np.int64)
        device_id = np.empty(max_rows, dtype=np.int64)
        module_id = np.empty(max_rows, dtype=np.int64)
        malformed_off = np.empty(self.MAX_MALFORMED, dtype=np.int64)
        malformed_len = np.empty(self.MAX_MALFORMED, dtype=np.int64)

        # Device/module id resolution: two Numba-backed string tables, local to this run() call
        # but keyed by the exact same global ids id_registry would hand out. Devices are one
        # flat, globally-unique-name table; modules are only unique *within* a device (two
        # devices can share a module name), so the module table is scoped per-row via
        # nb_resolve_scoped_id's device_id check (see ops/id_resolution.py) instead of needing
        # one table per device. A genuinely new name still has to go through the real
        # id_registry once (so live readers/the GUI see the same id) - resolve_temp_ids below
        # detects the Numba kernels' temp-id placeholders and does that, then mirrors the result
        # into the local table so every later occurrence resolves entirely in Numba.
        _TRACKER_CAPACITY = 64
        device_table = IndexedStringTable(initial_capacity=16, buffer_size_bytes=1024, use_hashes=True)
        module_table = IndexedStringTable(
            initial_capacity=64, buffer_size_bytes=8192, use_hashes=True, values_dtype=dtypes.ID_TYPE
        )
        device_identities: dict = {}  # device_id -> DeviceIdentity, for module resolution's get_module() call

        def make_tracker(name_bytes):
            return ModuleTrackerState(
                count=np.zeros(1, dtype=dtypes.ID_TYPE),
                bytes_cursor=np.zeros(1, dtype=dtypes.OFFSET_TYPE),
                starts=np.zeros(_TRACKER_CAPACITY, dtype=dtypes.OFFSET_TYPE),
                lengths=np.zeros(_TRACKER_CAPACITY, dtype=dtypes.LEN_TYPE),
                hashes=np.zeros(_TRACKER_CAPACITY, dtype=dtypes.HASH_TYPE),
                name_bytes=name_bytes,
            )

        def resolve_temp_ids(buf, out_id, off_arr, len_arr, rows_found, on_new_name):
            """Patches every MODULE_TEMP_ID_BASE.. placeholder nb_resolve_names_batch/
            nb_resolve_scoped_names_batch left in out_id[:rows_found], resolving each distinct
            new name exactly once via on_new_name(name, row_index) -> real_id."""
            resolved_slots = {}
            for i in range(rows_found):
                raw = int(out_id[i])
                if raw < MODULE_TEMP_ID_BASE:
                    continue
                off, length = int(off_arr[i]), int(len_arr[i])
                name = bytes(buf[off : off + length]).decode("ascii", "replace")
                if raw < MODULE_ID_FULL:
                    slot = raw - MODULE_TEMP_ID_BASE
                    if slot in resolved_slots:
                        continue
                    resolved_slots[slot] = on_new_name(name, i)
                else:
                    # MODULE_ID_UNKNOWN (empty name) or MODULE_ID_FULL (tracker exhausted -
                    # shouldn't happen given real device/module cardinality): never got a
                    # tracker slot, so patch this one row directly instead of a vectorized swap.
                    out_id[i] = on_new_name(name, i)
            for slot, real_id in resolved_slots.items():
                temp = MODULE_TEMP_ID_BASE + slot
                sub = out_id[:rows_found]
                sub[sub == temp] = real_id

        def register_device(name, _row_index):
            identity = id_registry.get_device(name)
            device_table.register_name(identity.id, name)
            device_identities[identity.id] = identity
            return identity.id

        def register_module(name, row_index):
            owner_device_id = int(device_id[row_index])
            identity = device_identities[owner_device_id].get_module(name)
            module_table.register_name(identity.id, name, value=owner_device_id)
            return identity.id

        try:
            for part in self.log_parts:
                if stop_is_set():
                    break

                if os.path.getsize(part) == 0:
                    continue

                with ExitStack() as stack:
                    part_str = str(part)
                    if part_str.endswith(ARCHIVE_SUFFIX):
                        # Compressed (rotated-away or cleanly-closed) part - see
                        # storage/log_file_archive.py. Already an owned buffer, nothing to close.
                        buf = decompress_log_part_to_buffer(part)
                    else:
                        # Uncompressed - today's behavior, and any part that never got
                        # compressed (e.g. the process was killed before a clean shutdown).
                        f = stack.enter_context(open(part, "rb"))
                        mm = stack.enter_context(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_COPY))
                        buf = np.frombuffer(mm, dtype=np.uint8)
                    buf_len = buf.shape[0]
                    cursor = 0
                    # tracker.name_bytes must be the exact array Numba is comparing candidate
                    # name spans against (see ops/id_resolution.py) - since buf is stable for
                    # this whole part (unlike ADB's reused ring buffer), pointing straight at it
                    # avoids ever having to copy candidate names into a separate scratch buffer.
                    device_tracker = make_tracker(buf)
                    module_tracker = make_tracker(buf)

                    while cursor < buf_len:
                        if stop_is_set():
                            break

                        rows_found, cursor, malformed_found, malformed_overflow = nb_scan_unified_log_lines(
                            buf,
                            cursor,
                            max_rows,
                            ts_ns,
                            level,
                            dev_off,
                            dev_len,
                            mod_off,
                            mod_len,
                            msg_off,
                            msg_len,
                            malformed_off,
                            malformed_len,
                            self.MAX_MALFORMED,
                        )

                        if self.logger:
                            for i in range(malformed_found):
                                off, ln = int(malformed_off[i]), int(malformed_len[i])
                                line = bytes(buf[off : off + ln])
                                self.logger.warn(f"UnifiedLogReplay: unparseable line skipped: {line[:80]!r}")
                            if malformed_overflow:
                                self.logger.warn(
                                    f"UnifiedLogReplay: {malformed_overflow} further unparseable "
                                    "line(s) skipped in this batch"
                                )

                        if rows_found == 0:
                            continue

                        # Device/module string->id resolution: entirely in Numba for repeat
                        # names (nb_resolve_names_batch/nb_resolve_scoped_names_batch, see
                        # ops/id_resolution.py), falling back to the real id_registry only for
                        # genuinely new names (resolve_temp_ids, above) - id_registry itself is
                        # a plain-Python dict+RLock structure with no Numba-callable equivalent,
                        # so a brand-new name still needs one Python call to get the id every
                        # other reader/the GUI would agree on. Devices must be fully resolved
                        # (no temp placeholders left) before modules, since module lookup is
                        # scoped by each row's already-resolved device id.
                        nb_resolve_names_batch(
                            buf, dev_off, dev_len, rows_found, device_table.bundle(), device_tracker, device_id
                        )
                        resolve_temp_ids(buf, device_id, dev_off, dev_len, rows_found, register_device)
                        device_tracker.count[0] = 0

                        nb_resolve_scoped_names_batch(
                            buf,
                            mod_off,
                            mod_len,
                            device_id,
                            rows_found,
                            module_table.bundle(),
                            module_tracker,
                            module_id,
                        )
                        resolve_temp_ids(buf, module_id, mod_off, mod_len, rows_found, register_module)
                        module_tracker.count[0] = 0

                        row_start = 0
                        while row_start < rows_found:
                            remaining = rows_found - row_start
                            was_empty = batch.size == 0
                            pushed = nb_push_unified_log_rows(
                                batch.bundle,
                                buf,
                                ts_ns[row_start : row_start + remaining],
                                level[row_start : row_start + remaining],
                                device_id[row_start : row_start + remaining],
                                module_id[row_start : row_start + remaining],
                                msg_off[row_start : row_start + remaining],
                                msg_len[row_start : row_start + remaining],
                                remaining,
                            )
                            row_start += pushed

                            if row_start >= rows_found:
                                break

                            if pushed == 0 and was_empty:
                                # A brand-new, empty batch still can't fit this one row - it's
                                # larger than BUFFER_BYTES/MAX_BATCH_ROWS allow. Skip it rather
                                # than spin forever retrying the same row against empty batches.
                                if self.logger:
                                    self.logger.warn(
                                        "UnifiedLogReplay: message too large for batch buffer, "
                                        f"row skipped (msg_len={int(msg_len[row_start])})"
                                    )
                                row_start += 1
                                continue

                            with batch:
                                self.distribute(batch)
                            batch = batch_acquire()

                    # Drop every reference to the mmap'd view before the `with` block below
                    # closes the mmap - mmap.close() raises BufferError on Windows while an
                    # exported buffer is still alive, and the trackers' name_bytes field (see
                    # make_tracker above) holds one too, not just the local `buf` name.
                    del buf, device_tracker, module_tracker

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
