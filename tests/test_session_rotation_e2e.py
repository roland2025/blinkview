# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for runtime session rotation (plans/session-rotation.md, phase 1): a real,
started Registry whose central FileLogger writes the unified log, rows pushed through central
storage before and after Registry.rotate_session(), and the resulting session folders checked on
disk after a real stop() - the level where a seam between FileManager, FileLogger and Registry
would actually show up."""

import json
import time

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.zstd_file_compression import decompress_file_to_buffer
from blinkview.utils.session_lister import list_sessions
from tests.fakes.real_registry import make_real_registry


def _wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


def _push(registry, texts):
    device = registry.id_registry.get_device("rotdev")
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


def _wait_logged(registry, texts):
    """Waits until central has stored `texts` (and so distributed them to its FileLogger) and the
    FileLogger's queue has drained - a batch it has taken off the queue is always processed before
    it can see a rotation request or a stop (both checked before the next batch). Counting popped
    rows instead doesn't work: the registry's own system-log rows go through the same queue."""
    queue = registry.central.file_logger.input_queue
    _wait_until(lambda: set(texts) <= _pool_messages(registry) and queue.get_stats()["total"] == 0)


def _unified_log_text(folder):
    parts = sorted(folder.glob("session.*"))
    assert parts, f"no unified log in {folder}"
    text = b""
    for part in parts:
        if part.suffix == ".zst":
            text += bytes(decompress_file_to_buffer(part))
        else:
            text += part.read_bytes()
    return text.decode("utf-8", errors="replace")


def _cold_storage_messages(session_dir):
    """Every message persisted in a session folder's cold storage (raw `cold/` segments and/or
    compressed `cold-archive/` ones), read back by mounting it in a fresh pool - the same way a
    later replay of that session would."""
    from blinkview.core.array_pool import NumpyArrayPool
    from blinkview.core.numpy_log import CircularLogPool

    pool = CircularLogPool(
        NumpyArrayPool(),
        max_pieces=2,
        cold_max_pieces=1024,
        cold_storage_dir=str(session_dir / "cold"),
        persist_cold_storage=True,
    )
    try:
        messages = []
        with pool.get_snapshot() as segments:
            for seg in segments:
                b = seg.bundle
                for i in range(seg.size):
                    off, length = int(b.offsets[i]), int(b.lengths[i])
                    messages.append(bytes(b.buffer[off : off + length]).decode())
        return messages
    finally:
        pool.release_all()


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "rotation_test", start=True, with_value_tracker=True)
    yield reg
    reg.stop()


def test_rotation_splits_the_unified_log_and_metadata_into_two_sessions(registry, tmp_path):
    assert registry.central.file_logger is not None, "central logging must be on for this test"
    before = [f"before-{i}" for i in range(5)]
    _push(registry, before)
    _wait_logged(registry, before)

    old_dir, new_dir = registry.rotate_session()

    after = [f"after-{i}" for i in range(3)]
    _push(registry, after)
    _wait_logged(registry, after)
    registry.stop()

    old_log, new_log = _unified_log_text(old_dir), _unified_log_text(new_dir)
    for i in range(5):
        assert f"before-{i}" in old_log and f"before-{i}" not in new_log
    for i in range(3):
        assert f"after-{i}" in new_log and f"after-{i}" not in old_log

    old_meta = json.loads((old_dir / "metadata.json").read_text())
    new_meta = json.loads((new_dir / "metadata.json").read_text())
    assert old_meta["status"] == new_meta["status"] == "finished"  # new one finished by stop()
    assert old_meta["next_session_id"] == new_dir.name
    assert new_meta["previous_session_id"] == old_dir.name
    assert new_meta["loggers"]["session"]["last_part"] >= 0

    # Config snapshots: old gets `final` at rotation, new gets `start` then `final` at stop().
    assert any(p.name.endswith(".final.json") for p in old_dir.iterdir())
    assert any(p.name.endswith(".start.json") for p in new_dir.iterdir())
    assert registry.config.autosave_path.parent == new_dir

    # In-memory data went with the old session: its cold storage (compressed at rotation, with
    # the id mapping alongside) holds exactly the pre-rotation rows; the new one's (compressed at
    # stop) exactly the post-rotation ones.
    old_cold = [m for m in _cold_storage_messages(old_dir) if m.startswith(("before-", "after-"))]
    new_cold = [m for m in _cold_storage_messages(new_dir) if m.startswith(("before-", "after-"))]
    assert sorted(old_cold) == [f"before-{i}" for i in range(5)]
    assert sorted(new_cold) == [f"after-{i}" for i in range(3)]
    assert (old_dir / "cold" / "id_registry.json").is_file()
    assert list((old_dir / "cold-archive").glob("segment_*.blkseg.zst"))

    # Both show up as loadable sessions.
    listed = {s.path for s in list_sessions(tmp_path, registry.file_manager.project_name)}
    assert {old_dir, new_dir} <= listed


def test_rotation_empties_the_pool_and_resets_latest_values(registry):
    pool = registry.central.log_pool
    _push(registry, ["before-x"])
    _wait_until(lambda: "before-x" in _pool_messages(registry))
    seq_before = int(pool.latest_sequence())
    tracker = registry.module_value_tracker
    tracker.update()
    module = registry.id_registry.get_device("rotdev").get_module("mod")
    with tracker.get_snapshot() as snap:
        assert snap.get_message(module.id) == "before-x"

    registry.rotate_session()

    # Not necessarily empty - the registry's own system-log rows (e.g. "Session rotated")
    # belong to the new session and land immediately - but nothing from before the rotation.
    with pool.get_snapshot() as segments:
        seqs = [int(s) for seg in segments for s in seg.bundle.sequences[: seg.size]]
    assert all(seq > seq_before for seq in seqs)
    assert "before-x" not in _pool_messages(registry)
    with tracker.get_snapshot() as snap:
        assert snap.get_sequence(module.id) == 0

    _push(registry, ["after-x"])
    _wait_until(lambda: "after-x" in _pool_messages(registry))
    tracker.update()
    with tracker.get_snapshot() as snap:
        assert snap.get_message(module.id) == "after-x"
        assert snap.get_sequence(module.id) > seq_before  # numbering continued


def test_rotation_with_a_new_name(registry):
    _, new_dir = registry.rotate_session("Second run")
    assert new_dir.name.endswith("_Second_run")
    assert json.loads((new_dir / "metadata.json").read_text())["project"]["display_name"] == "Second_run"


def test_rotation_is_refused_in_replay_mode(registry):
    registry.replay_mode = True
    with pytest.raises(RuntimeError):
        registry.rotate_session()
