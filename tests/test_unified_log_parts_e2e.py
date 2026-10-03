# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for unified_log_parts() against a session folder written by a real
FileLogger, in the state where a part exists both plain and compressed with *different*
content: the shutdown-time unlink failed (on Windows, anything holding the file open), so
FileLogger kept the part index and a restart() went on appending to the plain file. Per-function
tests over hand-made folders can't show that the plain file is the live superset - only running
the real FileLogger through stop/restart does."""

import re
import time
from pathlib import Path

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.storage.log_file_archive import decompress_log_part_to_buffer
from blinkview.utils.session_lister import SessionInfo, unified_log_parts
from tests.fakes.real_registry import make_real_registry

_PLAIN_PART_RE = re.compile(r"session\.\d{4,}\.log")


def _wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


def _push(registry, texts):
    device = registry.id_registry.get_device("partdev")
    module = device.get_module("mod")
    src = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(texts), 4096, has_levels=True, has_modules=True, has_devices=True
    )
    with src:
        for text in texts:
            ts = registry.now_ns()
            src.insert_any(ts, ts, text.encode("ascii"), level=0, module=module.id, device=device.id)
        registry.central.put(src)


def _pool_messages(registry):
    messages = set()
    with registry.central.log_pool.get_snapshot() as segments:
        for seg in segments:
            b = seg.bundle
            for i in range(seg.size):
                off, length = int(b.offsets[i]), int(b.lengths[i])
                messages.add(bytes(b.buffer[off : off + length]).decode(errors="replace"))
    return messages


def _wait_queued_rows_written(registry, texts):
    """Waits until central has stored `texts` and the FileLogger has taken every batch off its
    queue - a dequeued batch is always processed before the logger can see a stop (see
    test_session_rotation_e2e._wait_logged)."""
    queue = registry.central.file_logger.input_queue
    _wait_until(lambda: set(texts) <= _pool_messages(registry) and queue.get_stats()["total"] == 0)


def _part_text(part: Path) -> str:
    if part.name.endswith(".zst"):
        return bytes(decompress_log_part_to_buffer(part)).decode("utf-8", errors="replace")
    return part.read_bytes().decode("utf-8", errors="replace")


def _on_disk(path: Path, texts) -> bool:
    content = path.read_bytes().decode("utf-8", errors="replace") if path.exists() else ""
    return all(t in content for t in texts)


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "parts_test", start=True)
    yield reg
    reg.stop()


def test_plain_part_appended_after_a_failed_unlink_wins_over_its_stale_zst(registry, monkeypatch):
    file_logger = registry.central.file_logger
    assert file_logger is not None, "central logging must be on for this test"
    session_dir = file_logger.file_path.parent
    plain = session_dir / "session.0000.log"

    before = [f"before-{i}" for i in range(5)]
    _push(registry, before)
    _wait_queued_rows_written(registry, before)  # stop() flushes them, ahead of its compression

    # The shutdown-time compression succeeds, but the plain part can't be deleted.
    unlink_blocked = True
    real_unlink = Path.unlink

    def unlink(self, *args, **kwargs):
        if unlink_blocked and _PLAIN_PART_RE.fullmatch(self.name):
            raise PermissionError(f"simulated: {self.name} is open elsewhere")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    file_logger.flush_interval = 0.05  # the profile's 10 s would outlast the wait below; run() reads it on start
    file_logger.restart()

    after = [f"after-{i}" for i in range(3)]
    _push(registry, after)
    _wait_until(lambda: _on_disk(plain, after))
    unlink_blocked = False

    compressed = session_dir / "session.0000.log.zst"
    assert compressed.exists(), "precondition: the shutdown compression wrote its archive"
    assert "after-" not in _part_text(compressed), "precondition: the archive predates the restart"

    info = SessionInfo(session_dir.name, session_dir, session_dir.name, "", "unknown", None, None, None)
    parts = unified_log_parts(info)

    assert [p.name for p in parts] == ["session.0000.log"]
    text = "".join(_part_text(p) for p in parts)
    for message in before + after:
        assert text.count(f": {message}\n") == 1, message
