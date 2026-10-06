# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Progress reporting and cancellation for NumbaWarmupHelper.run_all() / Registry.warmup(), which
the GUI runs on a background thread behind a progress toast (plans/background-warmup.md)."""

import threading

import pytest

from blinkview.core import warmup_registry
from blinkview.core.registry import Registry
from blinkview.core.warmup import NumbaWarmupHelper, WarmupCancelled, warmup_label
from blinkview.core.warmup_registry import DEFAULT_WEIGHT, register_warmup


@pytest.fixture(autouse=True)
def clean_registry():
    warmup_registry._WARMUP_CALLBACKS.clear()
    warmup_registry._WARMUP_WEIGHTS.clear()
    yield
    warmup_registry._WARMUP_CALLBACKS.clear()
    warmup_registry._WARMUP_WEIGHTS.clear()


class FakeHelper:
    """Stands in for NumbaWarmupHelper's dummy environment; run_all() only needs log_pool."""

    def __init__(self):
        self.log_pool = self
        self.released = False

    def release_all(self):
        self.released = True

    def report_substep(self, done, total):
        NumbaWarmupHelper.report_substep(self, done, total)


def run(on_progress=None, cancel=None):
    helper = FakeHelper()
    NumbaWarmupHelper.run_all(helper, on_progress=on_progress, cancel=cancel)
    return helper


class TestWeights:
    def test_default_weight_is_recorded(self):
        @register_warmup
        def cb(helper):
            pass

        assert warmup_registry._WARMUP_WEIGHTS[cb] == DEFAULT_WEIGHT

    def test_explicit_weight_is_recorded_and_does_not_change_the_tuple(self):
        @register_warmup(priority=5, weight=40.0)
        def cb(helper):
            pass

        assert warmup_registry._WARMUP_CALLBACKS == [(5, cb)]
        assert warmup_registry._WARMUP_WEIGHTS[cb] == 40.0

    def test_run_all_clears_the_weights_afterward(self):
        @register_warmup(weight=3.0)
        def cb(helper):
            pass

        run()

        assert warmup_registry._WARMUP_WEIGHTS == {}


class TestLabel:
    def test_warmup_method_is_labelled_by_its_class(self):
        class CircularLogPool:
            @staticmethod
            def warmup(helper):
                pass

        assert warmup_label(CircularLogPool.warmup) == "CircularLogPool"

    def test_plain_function_keeps_its_qualname(self):
        def compile_things(helper):
            pass

        assert warmup_label(compile_things).endswith("compile_things")


class TestProgress:
    def test_reports_each_callback_start_and_a_final_one(self):
        @register_warmup
        def first(helper):
            pass

        @register_warmup
        def second(helper):
            pass

        events = []
        run(on_progress=lambda fraction, label: events.append((round(fraction, 3), label)))

        assert [e[0] for e in events] == [0.0, 0.5, 1.0]
        assert events[0][1].endswith("first")
        assert events[1][1].endswith("second")
        assert events[2] == (1.0, "")

    def test_weights_skew_the_fraction(self):
        @register_warmup(weight=3.0)
        def heavy(helper):
            pass

        @register_warmup(weight=1.0)
        def light(helper):
            pass

        fractions = []
        run(on_progress=lambda fraction, label: fractions.append(fraction))

        assert fractions == [0.0, 0.75, 1.0]

    def test_substeps_move_within_the_callbacks_share(self):
        @register_warmup(weight=2.0)
        def looping(helper):
            for i in range(4):
                helper.report_substep(i, 4)

        @register_warmup(weight=2.0)
        def last(helper):
            pass

        fractions = []
        run(on_progress=lambda fraction, label: fractions.append(fraction))

        # looping owns [0, 0.5): its begin, then sub-steps 0/4..3/4 of that half.
        assert fractions == [0.0, 0.0, 0.125, 0.25, 0.375, 0.5, 1.0]

    def test_fraction_never_decreases(self):
        @register_warmup(priority=10, weight=5.0)
        def a(helper):
            helper.report_substep(1, 2)

        @register_warmup(weight=1.0)
        def b(helper):
            helper.report_substep(3, 3)

        fractions = []
        run(on_progress=lambda fraction, label: fractions.append(fraction))

        assert fractions == sorted(fractions)
        assert fractions[-1] == 1.0

    def test_report_substep_outside_run_all_is_a_no_op(self):
        NumbaWarmupHelper.report_substep(FakeHelper(), 1, 2)  # unit tests run callbacks directly

    def test_no_progress_callback_is_fine(self):
        calls = []

        @register_warmup
        def cb(helper):
            helper.report_substep(0, 1)
            calls.append(True)

        run()

        assert calls == [True]


class TestCancel:
    def test_cancel_before_the_next_callback(self):
        cancel = threading.Event()
        calls = []

        @register_warmup
        def first(helper):
            calls.append("first")
            cancel.set()

        @register_warmup
        def second(helper):
            calls.append("second")

        with pytest.raises(WarmupCancelled):
            run(cancel=cancel)

        assert calls == ["first"]

    def test_cancel_at_a_substep(self):
        cancel = threading.Event()
        done = []

        @register_warmup
        def looping(helper):
            for i in range(5):
                helper.report_substep(i, 5)
                done.append(i)
                if i == 1:
                    cancel.set()

        with pytest.raises(WarmupCancelled):
            run(cancel=cancel)

        assert done == [0, 1]

    def test_cancelled_run_still_cleans_up(self):
        cancel = threading.Event()
        cancel.set()

        @register_warmup
        def cb(helper):
            pass

        helper = FakeHelper()
        with pytest.raises(WarmupCancelled):
            NumbaWarmupHelper.run_all(helper, cancel=cancel)

        assert helper.released is True
        assert warmup_registry._WARMUP_CALLBACKS == []
        assert helper._progress is None


class FakeRegistry:
    """The attributes Registry.warmup() touches, without building a real Registry."""

    class _Logger:
        def __init__(self):
            self.lines = []

        def warn(self, msg, *args):
            self.lines.append(msg % args if args else msg)

        def exception(self, msg, exc=None):
            self.lines.append(msg)

    def __init__(self, run_all):
        self._warmup_lock = threading.Lock()
        self._warmup_done = False
        self.warmup_success = False
        self.warmup_error = None
        self.warmup_helper = None
        self.logger = self._Logger()
        self._run_all = run_all

    def get_warmup(self):
        registry = self

        class _Helper:
            def run_all(self, on_progress=None, cancel=None):
                registry._run_all(on_progress, cancel)

        return _Helper()


class TestRegistryWarmup:
    def test_passes_progress_and_cancel_through(self):
        seen = {}
        cancel = threading.Event()

        def run_all(on_progress, cancel_event):
            seen["cancel"] = cancel_event
            on_progress(1.0, "")

        progress = []
        reg = FakeRegistry(run_all)
        Registry.warmup(reg, on_progress=lambda f, label: progress.append(f), cancel=cancel)

        assert seen["cancel"] is cancel
        assert progress == [1.0]
        assert reg.warmup_success is True
        assert reg._warmup_done is True

    def test_cancelled_is_done_but_not_successful(self):
        def run_all(on_progress, cancel_event):
            raise WarmupCancelled()

        reg = FakeRegistry(run_all)
        Registry.warmup(reg)

        assert reg._warmup_done is True
        assert reg.warmup_success is False
        assert reg.warmup_error == "cancelled"

    def test_concurrent_call_waits_instead_of_running_a_second_warmup(self):
        started = threading.Event()
        release = threading.Event()
        runs = []

        def run_all(on_progress, cancel_event):
            runs.append(threading.current_thread().name)
            started.set()
            release.wait(5)

        reg = FakeRegistry(run_all)
        background = threading.Thread(target=Registry.warmup, args=(reg,), name="bg")
        background.start()
        assert started.wait(5)

        second = threading.Thread(target=Registry.warmup, args=(reg,), name="second")
        second.start()
        second.join(0.2)
        assert second.is_alive()  # blocked on the lock, not running its own warmup

        release.set()
        background.join(5)
        second.join(5)

        assert runs == ["bg"]
        assert reg.warmup_success is True
