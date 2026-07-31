# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""zstandard compression for raw session/source log part files written by FileLogger
(session.NNNN.log, src_XXXX.NNNN.bin) - see plans/expressive-sauteeing-sun.md.

Mirrors core/cold_archive.py's cold-segment compression: a part file stays uncompressed while
still being actively appended to (FileLogger.open_file/_flush), and only gets compressed once
it's done being written - on rotation (the just-closed part) or on FileLogger shutdown (the
final part) - see FileLogger.open_file/run in file_logger.py. Compressed in place (no separate
archive directory, unlike cold storage): `session.0000.log` -> `session.0000.log.zst`, in the
same session folder, since unified_log_parts()'s `session.*` glob (utils/session_lister.py)
already matches any suffix and part-index ordering is unaffected by it.

UnifiedLogReplay (parsers/unified_log_replay.py) decompresses a `.zst` part straight into an
owned buffer instead of mmap'ing it, and falls back to the existing mmap path for any part that
never got compressed (e.g. the process was killed before a clean shutdown) - so a session can
freely mix compressed and uncompressed parts."""

from pathlib import Path
from typing import Union

import numpy as np

from blinkview.core.zstd_file_compression import compress_file, decompress_file_to_buffer

ARCHIVE_SUFFIX = ".zst"


def compress_log_part_file(path: Union[str, Path]) -> Path:
    """Compresses one already-closed log part file in place into a `.zst`-suffixed sibling.
    Caller is responsible for deleting `path` once this returns successfully - mirrors
    core/cold_archive.py's compress_cold_storage_dir's compress-then-unlink pattern, so a caller
    can log-and-skip a failure per file without losing the original."""
    path = Path(path)
    archive_path = path.with_name(path.name + ARCHIVE_SUFFIX)
    compress_file(path, archive_path)
    return archive_path


def decompress_log_part_to_buffer(path: Union[str, Path]) -> np.ndarray:
    """Fully decompresses a `.zst` log part straight into an owned, writable uint8 numpy buffer -
    see core/zstd_file_compression.decompress_file_to_buffer for the streaming/no-extra-copy
    details."""
    return decompress_file_to_buffer(path)
