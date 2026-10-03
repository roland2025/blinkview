# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import os
import re
import subprocess
import sys
from argparse import ArgumentParser
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
    run_export,
    setup_export_parser,
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


def _session(root: Path, name: str, lines: list[bytes], created_at: str = "2026-10-01T12:00:00Z") -> Path:
    folder = root / name
    folder.mkdir(parents=True)
    (folder / "metadata.json").write_text(json.dumps({"session_id": name, "created_at": created_at}))
    _write_plain(folder, 0, lines)
    return folder


def _run(argv: list[str]):
    parser = ArgumentParser(prog="blink export")
    setup_export_parser(parser)
    run_export(parser.parse_args(argv))


SESSION_LINES = [_line(0), _log("system", "central.stats", "5 msg/s"), _line(1), _log("system", "gui.lag", "40 ms")]
KEPT_LINES = [SESSION_LINES[0], SESSION_LINES[2], SESSION_LINES[3]]


class TestCommand:
    def test_session_folder_to_stdout(self, tmp_path, capsysbinary):
        folder = _session(tmp_path, "20261001_120000_a", SESSION_LINES)

        _run([str(folder)])

        assert capsysbinary.readouterr().out == b"\n".join(KEPT_LINES) + b"\n"

    def test_include_stats_keeps_everything(self, tmp_path, capsysbinary):
        folder = _session(tmp_path, "20261001_120000_a", SESSION_LINES)

        _run([str(folder), "--include-stats"])

        assert capsysbinary.readouterr().out == b"\n".join(SESSION_LINES) + b"\n"

    def test_single_part_file_is_named_after_its_folder(self, tmp_path):
        folder = _session(tmp_path, "20261001_120000_a", SESSION_LINES)
        _write_plain(folder, 3, [_line(9)])  # alone, not a gap

        _run([str(folder / "session.0003.log"), "-o", str(tmp_path / "out")])

        assert (tmp_path / "out" / "20261001_120000_a.log").read_bytes() == _line(9) + b"\n"

    def test_several_sessions_to_outdir(self, tmp_path, capsysbinary):
        a = _session(tmp_path, "a", SESSION_LINES)
        b = _session(tmp_path, "b", [_line(5)])
        outdir = tmp_path / "new" / "out"

        _run([str(a), str(b), "-o", str(outdir)])

        assert (outdir / "a.log").read_bytes() == b"\n".join(KEPT_LINES) + b"\n"
        assert (outdir / "b.log").read_bytes() == _line(5) + b"\n"
        captured = capsysbinary.readouterr()
        assert captured.out == b""
        assert b"3 of 4 lines" in captured.err

    def test_partial_last_line_is_terminated_and_bytes_pass_through(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        lines = [_line(0) + b" \xff\xfe", _line(1)]
        _write_plain(folder, 0, lines, terminated=False)

        _run([str(folder)])

        assert capsysbinary.readouterr().out == b"\n".join(lines) + b"\n"

    def test_warnings_go_to_stderr_with_the_session_name(self, tmp_path, capsysbinary):
        folder = _session(tmp_path, "s", [_line(0)])
        _write_plain(folder, 2, [_line(2)])

        _run([str(folder)])

        captured = capsysbinary.readouterr()
        assert captured.out == _line(0) + b"\n" + _line(2) + b"\n"
        assert b"warning: s: missing parts 0001" in captured.err

    def test_mixed_plain_and_compressed_parts(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        _write_zst(folder, 0, [_line(0)])
        _write_plain(folder, 1, [_line(1)])

        _run([str(folder)])

        assert capsysbinary.readouterr().out == _line(0) + b"\n" + _line(1) + b"\n"


class TestSessionLookup:
    @pytest.fixture
    def log_root(self, tmp_path):
        root = tmp_path / "logs"
        _session(root / "proj", "20261001_120000_old_bench", [_line(0)], created_at="2026-10-01T12:00:00Z")
        _session(root / "proj", "20261002_120000_new_astra", [_line(1)], created_at="2026-10-02T12:00:00Z")
        with patch.object(session_export, "resolve_log_root", return_value=(root, "proj")):
            yield root

    def test_by_id(self, log_root, capsysbinary):
        _run(["20261001_120000_old_bench"])

        captured = capsysbinary.readouterr()
        assert captured.out == _line(0) + b"\n"
        assert b"20261001_120000_old_bench: 20261001_120000_old_bench" in captured.err

    def test_partial_name_prints_what_it_resolved_to(self, log_root, capsysbinary):
        _run(["astra"])

        captured = capsysbinary.readouterr()
        assert captured.out == _line(1) + b"\n"
        assert b"astra: 20261002_120000_new_astra" in captured.err

    def test_last(self, log_root, capsysbinary):
        _run(["--last"])

        assert capsysbinary.readouterr().out == _line(1) + b"\n"

    def test_unknown_name_is_an_error(self, log_root):
        with pytest.raises(SystemExit, match="no session folder, part file or recorded session matching 'nope'"):
            _run(["nope"])

    def test_an_existing_path_wins_over_a_name(self, log_root, tmp_path, capsysbinary, monkeypatch):
        """A folder in cwd named like a recorded session is exported, not the recorded one."""
        _session(tmp_path / "cwd", "astra", [_line(7)])
        monkeypatch.chdir(tmp_path / "cwd")

        _run(["astra"])

        assert capsysbinary.readouterr().out == _line(7) + b"\n"


class TestCommandErrors:
    def test_nothing_to_export(self):
        with pytest.raises(SystemExit, match="give a SESSION"):
            _run([])

    def test_last_with_sessions(self, tmp_path):
        with pytest.raises(SystemExit, match="not both"):
            _run(["--last", str(tmp_path)])

    def test_several_sessions_without_outdir(self, tmp_path):
        a = _session(tmp_path, "a", [_line(0)])
        b = _session(tmp_path, "b", [_line(1)])

        with pytest.raises(SystemExit, match="several sessions need -o"):
            _run([str(a), str(b)])

    def test_output_name_clash_writes_nothing(self, tmp_path):
        folder = _session(tmp_path, "s", [_line(0)])
        _write_plain(folder, 1, [_line(1)])
        outdir = tmp_path / "out"

        with pytest.raises(SystemExit, match="same output file: s"):
            _run([str(folder / "session.0000.log"), str(folder / "session.0001.log"), "-o", str(outdir)])

        assert not outdir.exists()

    def test_folder_without_parts(self, tmp_path):
        with pytest.raises(SystemExit, match="no session.NNNN.log"):
            _run([str(tmp_path)])


def _subprocess_env(tmp_path: Path) -> dict:
    env = dict(os.environ)
    env["HOME"] = env["USERPROFILE"] = str(tmp_path / "home")
    env["BLINK_PROJECT_ROOT"] = str(tmp_path / "no_project")
    return env


class TestFreshProcess:
    def test_export_does_not_import_numba_qt_or_the_pipeline(self, tmp_path):
        """Must be a subprocess: in-process, sys.modules holds whatever other tests imported."""
        folder = tmp_path / "s"
        folder.mkdir()
        _write_zst(folder, 0, [_line(0)])
        _write_plain(folder, 1, [_line(1)])
        code = (
            "import sys, atexit\n"
            "atexit.register(lambda: sys.stderr.write('MODULES:' + ','.join(sorted(sys.modules)) + '\\n'))\n"
            f"sys.argv = ['blink', 'export', {str(folder)!r}, '-o', {str(tmp_path / 'out')!r}]\n"
            "from blinkview.__main__ import main\n"
            "main()\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code], env=_subprocess_env(tmp_path), capture_output=True, text=True, timeout=60
        )

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "out" / "s.log").read_bytes() == _line(0) + b"\n" + _line(1) + b"\n"
        modules = result.stderr.split("MODULES:")[1].strip().split(",")
        heavy = [
            m
            for m in modules
            if m.split(".")[0] in ("numba", "llvmlite", "PySide6", "qtpy")
            or m.startswith(("blinkview.parsers", "blinkview.ops", "blinkview.storage", "blinkview.core.id_registry"))
        ]
        assert heavy == []

    def test_piping_into_a_reader_that_stops_early_exits_quietly(self, tmp_path):
        """`blink export X | head`. Enough output to overflow the pipe buffer, so the writer is
        still writing when the reader goes away."""
        folder = tmp_path / "s"
        folder.mkdir()
        _write_plain(folder, 0, [_line(i) for i in range(200_000)])

        proc = subprocess.Popen(
            [sys.executable, "-m", "blinkview", "export", str(folder)],
            env=_subprocess_env(tmp_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout.read(100)
        proc.stdout.close()
        stderr = proc.stderr.read()
        returncode = proc.wait(timeout=60)

        assert b"Traceback" not in stderr, stderr.decode(errors="replace")
        assert returncode == 0
