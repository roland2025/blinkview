# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from contextlib import contextmanager

import numpy as np


class FakeSegment:
    def __init__(self, bundle, metadata=None):
        self.bundle = bundle
        self.size = int(bundle.size[0])
        self.last_sequence_id = int(bundle.sequences[-1]) if self.size else 0
        self.first_sequence_id = int(bundle.sequences[0]) if self.size else 0
        self.start_ts = int(bundle.timestamps[0]) if self.size else 9223372036854775807
        self.end_ts = int(bundle.timestamps[-1]) if self.size else -9223372036854775808
        self.metadata = metadata


class FakeIndicesHandle:
    def __init__(self, capacity=4096):
        self.array = np.zeros(capacity, dtype=np.int64)


class FakeLogPool:
    def __init__(self, latest_seq=0, segments=None):
        self._latest_seq = latest_seq
        self._segments = segments or []  # chronological order (oldest first)

    def latest_sequence(self):
        return self._latest_seq

    @contextmanager
    def get_reversed_snapshot(self):
        yield list(reversed(self._segments))

    @contextmanager
    def get_reversed_snapshot_since(self, last_known_seq):
        """Mirrors CircularLogPool.get_reversed_snapshot_since: a segment with cold-style
        metadata (has a `.last_seq`) that's already <= last_known_seq, and everything older than
        it, is skipped - a segment with no such metadata (hot-like, metadata=None) is always
        included, matching the real method's "always retain hot" behavior."""
        relevant = []
        for seg in reversed(self._segments):
            meta = seg.metadata
            if meta is not None and getattr(meta, "last_seq", None) is not None and meta.last_seq <= last_known_seq:
                break
            relevant.append(seg)
        yield relevant

    @contextmanager
    def get_snapshot(self):
        yield list(self._segments)

    @contextmanager
    def acquire_indices_buffer(self):
        yield FakeIndicesHandle()
