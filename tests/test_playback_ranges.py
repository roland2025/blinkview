# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.playback_ranges import PlaybackRange, PlaybackRangeStore


class TestPlaybackRange:
    def test_normalized_swaps_out_of_order_bounds(self):
        rng = PlaybackRange(id="a", name="x", start_ts_ns=500, end_ts_ns=100)
        norm = rng.normalized()
        assert (norm.start_ts_ns, norm.end_ts_ns) == (100, 500)

    def test_normalized_is_noop_for_already_ordered_bounds(self):
        rng = PlaybackRange(id="a", name="x", start_ts_ns=100, end_ts_ns=500)
        assert rng.normalized() == rng


class TestPlaybackRangeStore:
    def test_add_normalizes_and_assigns_id(self):
        store = PlaybackRangeStore()
        rng = store.add("boot sequence", 500, 100)

        assert rng.name == "boot sequence"
        assert (rng.start_ts_ns, rng.end_ts_ns) == (100, 500)
        assert rng.id
        assert store.ranges == [rng]

    def test_remove_by_id(self):
        store = PlaybackRangeStore()
        a = store.add("a", 0, 10)
        store.add("b", 10, 20)

        assert store.remove(a.id) is True
        assert [r.name for r in store.ranges] == ["b"]
        assert store.remove("does-not-exist") is False

    def test_rename(self):
        store = PlaybackRangeStore()
        a = store.add("a", 0, 10)

        assert store.rename(a.id, "renamed") is True
        assert store.get(a.id).name == "renamed"
        assert store.rename("nope", "x") is False

    def test_on_change_fires_on_every_mutation_not_on_reads(self):
        calls = []
        store = PlaybackRangeStore(on_change=lambda: calls.append(1))

        rng = store.add("a", 0, 10)
        assert len(calls) == 1

        store.rename(rng.id, "b")
        assert len(calls) == 2

        _ = store.ranges  # read-only access must not notify
        _ = store.get(rng.id)
        assert len(calls) == 2

        store.remove(rng.id)
        assert len(calls) == 3

    def test_clear_notifies_only_when_non_empty(self):
        calls = []
        store = PlaybackRangeStore(on_change=lambda: calls.append(1))

        store.clear()  # nothing to clear yet
        assert calls == []

        store.add("a", 0, 10)
        store.clear()
        assert len(calls) == 2  # one for add, one for clear
        assert store.ranges == []


class TestJsonRoundTrip:
    def test_to_json_data_and_load_json_data_round_trip(self):
        store = PlaybackRangeStore()
        store.add("boot", 0, 100)
        store.add("crash", 200, 300)

        data = store.to_json_data()

        restored = PlaybackRangeStore()
        restored.load_json_data(data)

        assert restored.ranges == store.ranges

    def test_load_json_data_replace_true_discards_existing(self):
        store = PlaybackRangeStore()
        store.add("stale", 0, 10)

        store.load_json_data({"version": 1, "ranges": []}, replace=True)

        assert store.ranges == []

    def test_load_json_data_replace_false_merges_by_id(self):
        store = PlaybackRangeStore()
        keep = store.add("keep", 0, 10)

        incoming = {
            "version": 1,
            "ranges": [{"id": "external-id", "name": "from source session", "start_ts_ns": 5, "end_ts_ns": 15}],
        }
        store.load_json_data(incoming, replace=False)

        names = sorted(r.name for r in store.ranges)
        assert names == ["from source session", "keep"]
        assert store.get(keep.id) is not None


class TestFilePersistence:
    def test_save_then_load_round_trips(self, tmp_path):
        path = tmp_path / "playback_ranges.json"
        store = PlaybackRangeStore()
        store.add("a", 0, 100)
        store.save_to_file(path)

        loaded = PlaybackRangeStore()
        result = loaded.load_from_file(path)

        assert result is True
        assert loaded.ranges == store.ranges

    def test_load_from_missing_file_returns_false_without_raising(self, tmp_path):
        store = PlaybackRangeStore()
        result = store.load_from_file(tmp_path / "does_not_exist.json")

        assert result is False
        assert store.ranges == []

    def test_load_from_corrupt_file_returns_false_without_raising(self, tmp_path):
        path = tmp_path / "corrupt.json"
        path.write_text("{not valid json", encoding="utf-8")

        store = PlaybackRangeStore()
        result = store.load_from_file(path)

        assert result is False

    def test_on_change_fires_during_load(self):
        calls = []
        store = PlaybackRangeStore(on_change=lambda: calls.append(1))
        store.load_json_data({"version": 1, "ranges": [{"id": "x", "name": "n", "start_ts_ns": 0, "end_ts_ns": 1}]})
        assert len(calls) == 1
