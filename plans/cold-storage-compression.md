# zstandard compression for persisted cold storage

**Status**: implemented.

## Problem

`cold_storage_persist_on_close` (see `plans/mmap-coldstore.md`) leaves a session's cold-tier
segments as raw, uncompressed `.blkseg` files under `<session>/cold/` forever, so a machine with
many persisted sessions accumulates disk usage fast. The live cold tier itself can't be
compressed - a running session mmaps those files directly for scrubback, so they must stay
plain, uncompressed, directly-mmap-able files while in active use.

## Design

Three steps:

1. **Write raw to `cold/`** (already existed, unchanged) - `ColdStorageArchiver`/
   `write_cold_segment_file` keep writing plain `.blkseg` files while a session is live.
2. **Compress into `cold-archive/` at session close** - `Registry.stop()`, right after
   `log_pool.release_all()` has closed every mmap over those files (and right next to the
   existing `_dump_id_registry` step, same `cold_dir` local), calls the new
   `Registry._compress_persisted_cold_storage(cold_dir)` -> `cold_archive.compress_cold_storage_dir`.
   Every `segment_*.blkseg` in `cold_dir` gets zstd-compressed into a sibling
   `cold_dir.parent / "cold-archive"` directory and deleted from `cold_dir`. Best-effort per
   file - a failure on one segment is logged and skipped, never silently drops data (the
   uncompressed original is left in place instead).
3. **Mount straight from the archive, on replay** - `CircularLogPool._mount_existing_cold_segments`
   (core/numpy_log.py) now looks in both `cold_dir` (raw files, mmap'd as always via
   `PooledLogBatch.from_memmap`) and the sibling `cold-archive/` directory for any segment that
   doesn't already have a raw copy - those are decompressed straight into an owned in-memory
   buffer (`cold_archive.decompress_cold_segment_archive` ->
   `PooledLogBatch.from_compressed_archive`) and never touch disk for the decompressed bytes at
   all. **Revised from the original design**: an earlier version of this feature decompressed to
   a temp file in `cold_dir` and then mmap'd it back in - a pure write-then-reread round trip,
   since the whole file has to be read into memory to decompress it either way. Decompressing
   straight into the destination buffer removes that extra disk write+read entirely. See
   `plans/lazy-cold-segment-unpacking.md` for the (not pursued) alternative of only decompressing
   segments actually touched by scrubback, and why the research there led here instead.

   **Further refined**: the first version of the direct-to-RAM path still paid one extra memcpy -
   `zstandard`'s simplest decompression call returns an immutable `bytes` object, copied into a
   `bytearray` afterward for writability. `compress_cold_segment_file` now passes `size=` (the raw
   file's own on-disk size) so the zstd frame embeds its decompressed content size;
   `decompress_cold_segment_archive` reads that back cheaply from just the frame header (a 32-byte
   probe, not a real decompress) via `zstandard.get_frame_parameters()`, preallocates an
   exactly-sized writable `np.uint8` buffer, and decompresses straight into it via the streaming
   reader's `readinto()` - one pass, no intermediate copy. Falls back to the old grow-as-you-go
   `read()` if a frame's content size comes back as one of zstd's
   `CONTENTSIZE_UNKNOWN`/`CONTENTSIZE_ERROR` sentinels (an archive predating this change, or from
   a foreign tool) - verified empirically that both the size-embedding and the sentinel values
   themselves behave as expected before relying on them (measured ~3x faster than the
   bytearray-copy version on a 128MB segment, on top of removing the copy itself).

`id_registry.json` (device/module name mapping dumped alongside persisted cold storage) is left
alone - it's small JSON, not worth compressing, and the glob patterns used here (`segment_*.blkseg`
/ `segment_*.blkseg.zst`) never match it.

## Scope

Only the default `<session>/cold/` + `<session>/cold-archive/` layout - matches
`cold_storage_persist_on_close`'s own documented scope. An overridden `cold_storage_dir` gets a
fresh uniquely-named subdirectory every run and is never reopened (see
`CentralStorage._resolve_cold_storage_dir`'s docstring), so there would be nothing to unpack back
into even if compressed.

## Decisions made without asking

- **Compression timing**: synchronous, at `Registry.stop()` (same as the pre-existing
  `_dump_id_registry` step), not backgrounded. A session with many large persisted segments will
  see a slower app-close; acceptable for a first pass, flagged here rather than adding background-
  compression/cancellation complexity that wasn't asked for.
- **Compression level**: `zstandard.ZstdCompressor()`'s own default (level 3) - not benchmarked or
  tuned against real segment content.
- **Dependency**: `zstandard>=0.22.0`, added to the `gui-core` extras (alongside `psutil`/`numpy`/
  `numba` - cold storage already requires those). Verified `zstandard` 0.25.0 ships per-CPython-
  version wheels (cp310-cp314) for Windows/macOS/manylinux/musllinux, covering this project's
  `requires-python = ">=3.10,<3.15"` on every platform target - no source build risk, same
  verification approach as `psutil` (see `plans/auto-hot-cold-memory-management.md`).

## Non-goals

- Not compressing the live cold tier while a session is running - it must stay directly mmap-able.
- Not touching `id_registry.json`.
- Not adding a compression-level config knob - `zstandard`'s default is used as-is.
