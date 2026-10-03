# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from pathlib import Path
from unittest.mock import patch

import pytest
import zstandard

from blinkview.storage.log_file_archive import compress_log_part_file
from blinkview.utils import session_export
from blinkview.utils.session_export import (
    STATS_LOGGER_NAMES,
    ExportFilter,
    SessionRead,
    iter_session_lines,
    keep_line,
)

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "blinkview"


def _line(i: int, device: str = "nrf", module: str = "app.main") -> bytes:
    return b"2026-10-01T12:00:%02d.%06dZ I %s %s: message %d" % (i % 60, i, device.encode(), module.encode(), i)


def _lines(start: int, count: int) -> list[bytes]:
    return [_line(i) for i in range(start, start + count)]


def _write_plain(folder: Path, index: int, lines: list[bytes], terminated: bool = True) -> Path:
    path = folder / f"session.{index:04d}.log"
    data = b"\n".join(lines) + (b"\n" if terminated and lines else b"")
    path.write_bytes(data)
    return path


def _write_zst(folder: Path, index: int, lines: list[bytes]) -> Path:
    plain = _write_plain(folder, index, lines)
    archive = compress_log_part_file(plain)
    plain.unlink()
    return archive


def _read(parts, check_gaps=True):
    report = SessionRead()
    lines = list(iter_session_lines(parts, report, check_gaps=check_gaps))
    return lines, report


class TestReadsParts:
    def test_plain_part(self, tmp_path):
        expected = _lines(0, 50)
        lines, report = _read([_write_plain(tmp_path, 0, expected)])

        assert lines == expected
        assert report.warning_lines() == []
        assert not report.parts[0].compressed

    def test_compressed_part(self, tmp_path):
        expected = _lines(0, 50)
        lines, report = _read([_write_zst(tmp_path, 0, expected)])

        assert lines == expected
        assert report.warning_lines() == []
        assert report.parts[0].compressed

    def test_mixed_parts_in_listed_order(self, tmp_path):
        """A live session: earlier parts rotated away and compressed, the current one plain."""
        parts = [
            _write_zst(tmp_path, 0, _lines(0, 10)),
            _write_zst(tmp_path, 1, _lines(10, 10)),
            _write_plain(tmp_path, 2, _lines(20, 10)),
        ]

        lines, report = _read(parts)

        assert lines == _lines(0, 30)
        assert [p.compressed for p in report.parts] == [True, True, False]
        assert report.warning_lines() == []

    def test_empty_part_yields_nothing_without_warning(self, tmp_path):
        lines, report = _read([_write_plain(tmp_path, 0, [])])

        assert lines == []
        assert report.warning_lines() == []

    def test_empty_lines_are_kept(self, tmp_path):
        expected = [_line(0), b"", _line(1)]
        lines, _ = _read([_write_plain(tmp_path, 0, expected)])

        assert lines == expected

    def test_invalid_utf8_passes_through_unchanged(self, tmp_path):
        expected = [_line(0) + b" \xff\xfe\xc3", _line(1)]
        lines, _ = _read([_write_zst(tmp_path, 0, expected)])

        assert lines == expected


class TestChunkBoundaries:
    @pytest.mark.parametrize("chunk", [1, 7, 64, 4096])
    def test_lines_crossing_plain_chunk_boundaries(self, tmp_path, chunk):
        expected = _lines(0, 40)
        with patch.object(session_export, "PLAIN_CHUNK_BYTES", chunk):
            lines, _ = _read([_write_plain(tmp_path, 0, expected)])

        assert lines == expected

    @pytest.mark.parametrize("chunk", [1, 13, 256])
    def test_lines_crossing_compressed_input_chunk_boundaries(self, tmp_path, chunk):
        expected = _lines(0, 400)
        part = _write_zst(tmp_path, 0, expected)
        with patch.object(session_export, "ZST_INPUT_CHUNK_BYTES", chunk):
            lines, report = _read([part])

        assert lines == expected
        assert report.warning_lines() == []


class TestPartialLastLine:
    def test_unterminated_last_line_is_yielded_and_flagged(self, tmp_path):
        expected = _lines(0, 5)
        lines, report = _read([_write_plain(tmp_path, 0, expected, terminated=False)])

        assert lines == expected
        assert report.parts[0].ends_partial

    def test_terminated_part_is_not_flagged(self, tmp_path):
        _, report = _read([_write_plain(tmp_path, 0, _lines(0, 5))])

        assert not report.parts[0].ends_partial

    def test_plain_part_is_read_only_up_to_its_size_at_open(self, tmp_path):
        """A live part keeps growing while it's read - bytes appended after open are not chased,
        and a line cut at that point is a partial last line."""
        part = _write_plain(tmp_path, 0, _lines(0, 3))
        real_open = open

        class GrowsOnFirstRead:
            """FileLogger appending while the export is reading."""

            def __init__(self, f):
                self._f = f
                self._grown = False

            def read(self, n):
                chunk = self._f.read(n)
                if not self._grown:
                    self._grown = True
                    with real_open(part, "ab") as writer:
                        writer.write(_line(3) + b"\n")
                return chunk

            def __getattr__(self, name):
                return getattr(self._f, name)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._f.close()

        def open_growing(path, mode="r", *args, **kwargs):
            return GrowsOnFirstRead(real_open(path, mode, *args, **kwargs))

        with patch.object(session_export, "PLAIN_CHUNK_BYTES", 16):
            with patch("blinkview.utils.session_export.open", open_growing, create=True):
                lines, report = _read([part])

        assert part.read_bytes().endswith(_line(3) + b"\n")  # it did grow
        assert lines == _lines(0, 3)
        assert not report.parts[0].ends_partial


class TestBrokenArchives:
    def test_truncated_archive_keeps_lines_up_to_the_break_and_reads_on(self, tmp_path):
        first = _lines(0, 20_000)
        archive = _write_zst(tmp_path, 0, first)
        data = archive.read_bytes()
        archive.write_bytes(data[: len(data) // 2])
        following = _write_plain(tmp_path, 1, _lines(20_000, 5))

        lines, report = _read([archive, following])

        kept_from_first = lines[: lines.index(_line(20_000))]
        assert 0 < len(kept_from_first) < len(first)
        # Every complete line kept is exact; only the very last one can be cut mid-line.
        assert kept_from_first[:-1] == first[: len(kept_from_first) - 1]
        assert report.parts[0].ends_partial
        assert "truncated" in report.parts[0].warning
        assert lines[-5:] == _lines(20_000, 5)
        assert report.parts[1].warning is None

    def test_corrupt_archive_warns_and_reads_on(self, tmp_path):
        """Corrupts the first block header, which zstd always detects. compress_file writes no
        content checksum, so corrupt bytes inside a block's literals decode silently into wrong
        text - no reader can catch that."""
        archive = _write_zst(tmp_path, 0, _lines(0, 20_000))
        data = bytearray(archive.read_bytes())
        data[20:84] = b"\xff" * 64
        archive.write_bytes(bytes(data))
        following = _write_plain(tmp_path, 1, _lines(20_000, 5))

        lines, report = _read([archive, following])

        assert "corrupt" in report.parts[0].warning
        assert lines[-5:] == _lines(20_000, 5)

    def test_zero_byte_archive_is_reported_as_truncated(self, tmp_path):
        archive = tmp_path / "session.0000.log.zst"
        archive.write_bytes(b"")

        lines, report = _read([archive])

        assert lines == []
        assert "truncated" in report.parts[0].warning

    def test_data_after_the_frame_is_ignored_with_a_warning(self, tmp_path):
        expected = _lines(0, 10)
        archive = _write_zst(tmp_path, 0, expected)
        archive.write_bytes(archive.read_bytes() + zstandard.ZstdCompressor().compress(b"second frame\n"))

        lines, report = _read([archive])

        assert lines == expected
        assert "after the end of the zstd frame" in report.parts[0].warning

    def test_data_after_the_frame_starting_on_a_chunk_boundary_is_detected(self, tmp_path):
        archive = _write_zst(tmp_path, 0, _lines(0, 10))
        frame_size = archive.stat().st_size
        archive.write_bytes(archive.read_bytes() + b"junk")

        with patch.object(session_export, "ZST_INPUT_CHUNK_BYTES", frame_size):
            _, report = _read([archive])

        assert "after the end of the zstd frame" in report.parts[0].warning


class TestVanishedParts:
    def test_plain_part_compressed_away_after_listing_is_read_from_its_archive(self, tmp_path):
        listed = tmp_path / "session.0000.log"
        archive = _write_zst(tmp_path, 0, _lines(0, 10))
        assert not listed.exists()

        lines, report = _read([listed])

        assert lines == _lines(0, 10)
        assert report.parts[0].path == archive
        assert report.parts[0].compressed
        assert report.warning_lines() == []

    def test_plain_part_unlinked_between_resolve_and_open_falls_back_to_its_archive(self, tmp_path):
        listed = _write_plain(tmp_path, 0, _lines(0, 10))
        archive = compress_log_part_file(listed)
        real_open = open

        def unlink_then_open(path, mode="r", *args, **kwargs):
            if Path(path) == listed and listed.exists():
                listed.unlink()  # FileLogger's unlink lands right between existing_part() and open()
            return real_open(path, mode, *args, **kwargs)

        with patch("blinkview.utils.session_export.open", unlink_then_open, create=True):
            lines, report = _read([listed])

        assert lines == _lines(0, 10)
        assert report.parts[0].path == archive

    def test_part_gone_entirely_warns_and_reads_on(self, tmp_path):
        gone = tmp_path / "session.0000.log"
        following = _write_plain(tmp_path, 1, _lines(10, 3))

        lines, report = _read([gone, following])

        assert lines == _lines(10, 3)
        assert report.parts[0].path is None
        assert report.warning_lines() == ["session.0000.log: vanished before it could be read, skipped"]


class TestGaps:
    def test_hole_in_numbering_warns(self, tmp_path):
        parts = [_write_plain(tmp_path, i, _lines(i, 1)) for i in (0, 1, 3, 4, 7)]

        _, report = _read(parts)

        assert report.warnings == ["missing parts 0002, 0005-0006"]

    def test_first_index_not_zero_warns(self, tmp_path):
        parts = [_write_plain(tmp_path, i, _lines(i, 1)) for i in (3, 4)]

        _, report = _read(parts)

        assert report.warnings == ["missing parts 0000-0002"]

    def test_gap_check_uses_listed_names_including_compressed_ones(self, tmp_path):
        parts = [_write_zst(tmp_path, 0, _lines(0, 1)), _write_plain(tmp_path, 1, _lines(1, 1))]

        _, report = _read(parts)

        assert report.warnings == []

    def test_single_part_file_skips_the_gap_check(self, tmp_path):
        _, report = _read([_write_plain(tmp_path, 5, _lines(0, 1))], check_gaps=False)

        assert report.warnings == []


def _log(device: str, module: str, message: str = "x") -> bytes:
    return f"2026-10-01T12:00:00.000000Z I {device} {module}: {message}".encode()


class TestStatsFilter:
    @pytest.mark.parametrize(
        "module",
        [
            "central.stats",
            "source.nrf_rtt.stats",
            "source.nrf_rtt.tuner",
            "parser.nrf.stats_in",
            "parser.nrf.stats_out",
            "parser.nrf.tuner_out",
            "reorder.stats_out",
            "stats",
        ],
    )
    def test_system_stats_lines_are_dropped(self, module):
        assert not keep_line(_log("system", module, "1234 msg/s"), ExportFilter())

    @pytest.mark.parametrize("module", ["statsd", "foo.stats_extra", "stats.collector", "app.tuner2"])
    def test_system_modules_that_only_resemble_stats_are_kept(self, module):
        assert keep_line(_log("system", module), ExportFilter())

    @pytest.mark.parametrize(
        "message",
        ["send_command: reset", "J-Link connection lost", "Target system has no power"],
    )
    def test_other_system_lines_are_kept(self, message):
        """Host actions that explain firmware behaviour."""
        assert keep_line(_log("system", "source.nrf_rtt", message), ExportFilter())

    def test_a_firmware_module_named_stats_is_kept(self):
        assert keep_line(_log("nrf", "stats"), ExportFilter())
        assert keep_line(_log("nrf", "app.stats"), ExportFilter())

    def test_include_stats_keeps_them(self):
        options = ExportFilter(include_stats=True)
        assert keep_line(_log("system", "central.stats"), options)
        assert keep_line(_log("system", "parser.nrf.tuner_out"), options)

    def test_a_module_token_without_its_colon_still_matches(self):
        assert not keep_line(b"2026-10-01T12:00:00.000000Z I system central.stats x", ExportFilter())

    @pytest.mark.parametrize("line", [b"", b"garbage", b"two fields", b"only three fields", b"\xff\xfe \x00"])
    def test_lines_with_fewer_than_four_fields_are_kept(self, line):
        assert keep_line(line, ExportFilter())

    def test_invalid_utf8_message_is_filtered_without_decoding(self):
        assert keep_line(_log("nrf", "app.main") + b" \xff\xfe", ExportFilter())
        assert not keep_line(_log("system", "central.stats") + b" \xff\xfe", ExportFilter())

    def test_names_match_every_stats_child_call_in_src(self):
        """A new logger.stats_child(name) would otherwise leak its lines into every export."""
        used = set()
        for path in SRC_ROOT.rglob("*.py"):
            used |= set(re.findall(r"stats_child\(\s*[\"']([^\"']+)[\"']", path.read_text(encoding="utf-8")))

        assert used, "found no stats_child calls - has the API been renamed?"
        assert used == set(STATS_LOGGER_NAMES)
