from contextlib import contextmanager

import numpy as np


class FakeSegment:
    def __init__(self, bundle):
        self.bundle = bundle
        self.size = int(bundle.size[0])
        self.last_sequence_id = int(bundle.sequences[-1]) if self.size else 0


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
    def get_snapshot(self):
        yield list(self._segments)

    @contextmanager
    def acquire_indices_buffer(self):
        yield FakeIndicesHandle()
