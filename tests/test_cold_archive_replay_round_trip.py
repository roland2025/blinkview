# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end proof of the full lifecycle a persisted-and-compressed session actually goes
through: live eviction to cold/ (unchanged), release_all() + compress_cold_storage_dir() at
session close (Registry.stop()'s new step), and CircularLogPool mounting straight from the
compressed archives on a later replay (CircularLogPool._mount_existing_cold_segments,
core/numpy_log.py) - driven at the CircularLogPool level (like
tests/test_numpy_log_cold_tier.py) rather than through a full Registry, to isolate this from
unrelated Registry/CentralStorage wiring already covered elsewhere."""

import time

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_archive import compress_cold_storage_dir
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool
from blinkview.utils.log_level import LogLevel


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def snapshot_seqs(pool):
    with pool.get_snapshot() as segments:
        return [int(s) for seg in segments for s in seg.bundle.sequences[: seg.size]]


def push_rows(pool, global_pool, count, ts_start):
    for i in range(count):
        batch = global_pool.create(
            PooledLogBatch,
            req_capacity=1,
            buffer_bytes=1,
            has_levels=True,
            has_modules=True,
            has_devices=True,
            has_sequences=True,
        )
        batch.insert(ts_start + i, ts_start + i, f"row{i}".encode(), LogLevel.INFO.value, 0, 0, 0)
        pool.batch_append(batch)
        batch.release()
        time.sleep(0.01)


@pytest.fixture
def global_pool():
    return NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024)


class TestCompressThenRemountFromArchive:
    def test_cold_tier_content_survives_a_full_compress_and_remount_cycle(self, global_pool, tmp_path):
        cold_dir = tmp_path / "session" / "cold"

        # --- Live session: evict to cold, then close with persistence on (the only case this
        # compression feature applies to - see cold_storage_persist_on_close). cold_max_pieces=4
        # (not 2) so release_all()'s hot-tier flush (below) doesn't itself evict-and-delete the
        # already-cold segments to stay under the cap - that eviction behavior is real and
        # correct, just not what this test is about.
        pool = CircularLogPool(
            global_pool,
            max_pieces=2,
            cold_max_pieces=4,
            cold_storage_dir=str(cold_dir),
            final_buffer_bytes=1024,
            persist_cold_storage=True,
        )
        pool.segment_capacity = 1
        pool._optimized = True
        pool.clear()

        push_rows(pool, global_pool, 4, ts_start=100)
        assert wait_for(lambda: snapshot_seqs(pool) == [1, 2, 3, 4])
        original_seqs = snapshot_seqs(pool)  # [1, 2, 3, 4]: seq 1,2 cold; seq 3,4 still hot

        # persist=True: also flushes the still-hot segments (seq 3, 4) to disk, not just the
        # already-cold ones - see release_all()'s docstring.
        pool.release_all()

        raw_files_before = sorted(cold_dir.glob("segment_*.blkseg"))
        assert len(raw_files_before) == 4

        # --- Session teardown's new step: shrink the persisted footprint. ---
        compress_cold_storage_dir(cold_dir)

        assert list(cold_dir.glob("segment_*.blkseg")) == []
        archive_dir = tmp_path / "session" / "cold-archive"
        assert len(list(archive_dir.glob("segment_*.blkseg.zst"))) == 4

        # --- A later replay's mount step: straight from the compressed archives, no disk
        # round-trip - _mount_existing_cold_segments decompresses each directly into RAM. ---
        remounted = CircularLogPool(
            global_pool,
            max_pieces=2,
            cold_max_pieces=4,
            cold_storage_dir=str(cold_dir),
            final_buffer_bytes=1024,
        )
        try:
            assert remounted.resumed_from_existing_cold_storage is True
            assert len(remounted.cold_segments) == 4
            # Mounting never wrote any decompressed bytes back to cold/ - the whole point.
            assert list(cold_dir.glob("segment_*.blkseg")) == []
            # All 4 rows' sequence numbers must survive byte-for-byte through the whole
            # evict -> release_all(persist) -> compress -> remount-from-archive cycle.
            remounted_seqs = [int(s) for seg in remounted.cold_segments for s in seg.bundle.sequences[: seg.size]]
            assert remounted_seqs == original_seqs
        finally:
            remounted.release_all()

    def test_a_raw_cold_file_takes_priority_over_its_archive_counterpart(self, global_pool, tmp_path):
        """If both cold/segment_N.blkseg and cold-archive/segment_N.blkseg.zst exist, mounting
        must prefer the raw file - see _mount_existing_cold_segments' docstring. Proven here by
        making the two deliberately disagree (different row content under the same filename) and
        asserting the mounted segment's content is the raw one's, not the archive's."""
        from blinkview.core.cold_segment import write_cold_segment_file

        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)
        archive_dir = tmp_path / "session" / "cold-archive"
        archive_dir.mkdir(parents=True)

        def make_batch(seq):
            batch = global_pool.create(
                PooledLogBatch, req_capacity=1, buffer_bytes=1, has_levels=True, has_modules=True,
                has_devices=True, has_sequences=True,
            )
            batch.insert(100 + seq, 100 + seq, b"x", LogLevel.INFO.value, 0, 0, seq)
            return batch

        # The raw file (seq 999) is what mounting must actually pick, not the archive (seq 111).
        raw_batch = make_batch(999)
        write_cold_segment_file(cold_dir / "segment_0000000000.blkseg", raw_batch.bundle)
        raw_batch.release()

        archive_batch = make_batch(111)
        scratch = tmp_path / "scratch_for_archive_only.blkseg"
        write_cold_segment_file(scratch, archive_batch.bundle)
        archive_batch.release()
        from blinkview.core.cold_archive import compress_cold_segment_file

        compress_cold_segment_file(scratch, archive_dir)  # -> segment_0000000000.blkseg.zst

        remounted = CircularLogPool(
            global_pool, max_pieces=1, cold_max_pieces=4, cold_storage_dir=str(cold_dir), final_buffer_bytes=1024
        )
        try:
            assert len(remounted.cold_segments) == 1
            seg = remounted.cold_segments[0]
            assert int(seg.bundle.sequences[0]) == 999
        finally:
            remounted.release_all()
