# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The Benchmark source's line caches: the original one is unchanged, and each format template parses cleanly
with the pipeline it is meant for, one row per frame, also when the cache is replayed."""

import random

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.io.benchmark import Benchmark, warm_up_blast
from blinkview.io.benchmark_templates import TEMPLATES, build_cache
from tests.test_frame_parser_equivalence import cut, run_parser


def _chunks(cache):
    return [bytes(cache.buf[o : o + n]) for o, n in zip(cache.offsets.tolist(), cache.lengths.tolist())]


def test_random_cache_is_the_original_one():
    cache = build_cache("random")

    lines = [b"Random: %d\n" % i for i in range(1000)]
    expected = [b"".join(lines[i : i + 50]) for i in range(0, 1000, 50)]
    assert _chunks(cache) == expected
    assert cache.frames_per_chunk == 50
    assert cache.offsets.dtype == np.uint32 and cache.lengths.dtype == np.uint32


def test_unknown_template_is_an_error():
    with pytest.raises(ValueError, match="zephyr_deferred"):
        build_cache("zephyr")


def test_benchmark_source_offers_every_template_and_defaults_to_random():
    prop = Benchmark.get_config_schema()["properties"]["template"]

    assert prop["enum"] == list(TEMPLATES)
    assert prop["default"] == "random"


@pytest.mark.parametrize("template", TEMPLATES)
def test_warm_up_blast_fits_every_template(template):
    """Benchmark.run()'s kernel warm-up used a 1 KB batch, which the ~3 KB template chunks overflowed."""
    cache = build_cache(template)

    written = warm_up_blast(NumpyArrayPool().create, 1, cache.buf, cache.offsets, cache.lengths)

    assert written == cache.lengths[0]


@pytest.mark.parametrize("template", ["zephyr_deferred", "adb_long"])
def test_every_chunk_is_whole_frames(template):
    cache = build_cache(template)
    for chunk in _chunks(cache):
        if template == "zephyr_deferred":
            assert chunk.count(b"\r\n") == cache.frames_per_chunk and chunk.endswith(b"\r\n")
        else:
            assert chunk.startswith(b"[ ") and chunk.endswith(b"\n\n")


@pytest.mark.parametrize("template, workload", [("zephyr_deferred", "zephyr"), ("adb_long", "adb")])
def test_template_parses_one_row_per_frame_with_its_pipeline(template, workload):
    cache = build_cache(template)
    frames = len(cache.lengths) * cache.frames_per_chunk
    rng = random.Random(1)
    raw = bytes(cache.buf) * 2  # the source replays the cache
    rows = run_parser(workload, cut(raw, rng, max_row=65536, max_rows_per_batch=8))

    # zephyr: the decoder's start-up resync drops the first line; adb: the last frame waits for a next header
    assert len(rows) == 2 * frames - 1
    # no frame or parser error rows (those keep the raw text, which starts with '[')
    assert not [r for r in rows if b"[" in r[0]]
    timestamps = [r[1] for r in rows]
    assert all(b > a for a, b in zip(timestamps, timestamps[1:]))
    assert len({r[4] for r in rows}) >= 30  # module names / tags resolved, not all "log" or "unknown"
