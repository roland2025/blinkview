# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time
from types import SimpleNamespace

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.logger import PrintLogger


def make_daemon():
    daemon = BaseDaemon()
    daemon.logger = PrintLogger("test.base_daemon")
    # BaseDaemon.run() is a no-op that returns instantly - real subclasses loop on
    # _stop_event, so tests that need the thread to actually be observably "running"
    # need the same shape, or is_running becomes a race against the thread finishing.
    daemon.run = lambda: daemon._stop_event.wait()
    return daemon


class FakeFileLogger:
    def __init__(self):
        self.started = False
        self.stopped = False
        self.cleared = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def clear_all_links(self):
        self.cleared = True


class FakeFileManager:
    def __init__(self):
        self.removed = []

    def remove_file_logger(self, file_logger):
        self.removed.append(file_logger)


class FakeFactories:
    def __init__(self, built):
        self._built = built
        self.build_calls = []

    def build(self, category, config, system_ctx=None, instance_ctx=None, **kwargs):
        self.build_calls.append((category, config))
        return self._built


def make_daemon_with_shared(file_logger=None):
    daemon = make_daemon()
    daemon.name = "test-daemon"
    daemon.shared = SimpleNamespace(
        factories=FakeFactories(file_logger or FakeFileLogger()),
        registry=SimpleNamespace(file_manager=FakeFileManager(), logger_creator=lambda *a, **kw: lambda: None),
    )
    daemon.local = SimpleNamespace(logging_id="log-1")
    return daemon


class TestDefaults:
    def test_starts_disabled_and_not_running(self):
        daemon = make_daemon()
        assert daemon.enabled is False
        assert daemon.is_running is False

    def test_subscribers_and_children_start_empty(self):
        daemon = make_daemon()
        assert daemon.subscribers == []
        assert daemon._children == []
        assert daemon.file_logger is None


class TestDefaultRun:
    def test_default_run_is_a_noop(self):
        daemon = BaseDaemon()
        daemon.logger = PrintLogger("test.base_daemon")
        assert daemon.run() is None


class TestStartStop:
    def test_start_is_a_noop_when_not_enabled(self):
        daemon = make_daemon()
        daemon.enabled = False
        daemon.start()
        assert daemon.is_running is False

    def test_start_runs_the_thread_when_enabled(self):
        daemon = make_daemon()
        daemon.enabled = True
        daemon.start()
        try:
            assert daemon.is_running is True
        finally:
            daemon.stop()

    def test_start_is_a_noop_when_already_running(self):
        daemon = make_daemon()
        daemon.enabled = True
        daemon.start()
        try:
            first_thread = daemon._thread
            daemon.start()
            assert daemon._thread is first_thread
        finally:
            daemon.stop()

    def test_stop_joins_the_thread_and_clears_the_reference(self):
        daemon = make_daemon()
        daemon.enabled = True
        daemon.start()

        daemon.stop()

        assert daemon.is_running is False
        assert daemon._thread is None

    def test_stop_is_a_noop_when_not_running(self):
        daemon = make_daemon()
        daemon.stop()  # must not raise
        assert daemon.is_running is False

    def test_restart_stops_and_starts_again(self):
        daemon = make_daemon()
        daemon.enabled = True
        daemon.start()
        try:
            first_thread = daemon._thread
            daemon.restart()
            assert daemon._thread is not None
            assert daemon._thread is not first_thread
        finally:
            daemon.stop()


class TestRunWrapper:
    def test_exception_in_run_is_caught_and_logged_not_raised(self):
        daemon = make_daemon()

        def broken_run():
            raise RuntimeError("boom")

        daemon.run = broken_run
        daemon.enabled = True
        daemon.start()

        # Thread should exit (due to the exception) without the process crashing, and
        # is_running should settle back to False on its own.
        deadline = time.time() + 2.0
        while daemon.is_running and time.time() < deadline:
            time.sleep(0.02)

        assert daemon.is_running is False
        daemon.stop()


class TestSubscribers:
    def test_subscribe_adds_a_subscriber_once(self):
        daemon = make_daemon()
        sub = object()

        daemon.subscribe(sub)
        daemon.subscribe(sub)

        assert daemon.subscribers == [sub]

    def test_subscribe_tracks_subscription_on_the_subscriber_if_supported(self):
        daemon = make_daemon()

        class TrackingSub:
            def __init__(self):
                self.sources = []

            def track_subscription(self, source_obj):
                self.sources.append(source_obj)

        sub = TrackingSub()
        daemon.subscribe(sub)

        assert sub.sources == [daemon]

    def test_unsubscribe_removes_a_subscriber(self):
        daemon = make_daemon()
        sub = object()
        daemon.subscribe(sub)

        daemon.unsubscribe(sub)

        assert daemon.subscribers == []

    def test_unsubscribe_unknown_subscriber_is_a_noop(self):
        daemon = make_daemon()
        daemon.unsubscribe(object())  # must not raise

    def test_distribute_puts_the_batch_on_every_subscriber(self):
        daemon = make_daemon()

        class RecordingSub:
            def __init__(self):
                self.received = []

            def put(self, batch):
                self.received.append(batch)

        sub_a, sub_b = RecordingSub(), RecordingSub()
        daemon.subscribe(sub_a)
        daemon.subscribe(sub_b)

        daemon.distribute("the-batch")

        assert sub_a.received == ["the-batch"]
        assert sub_b.received == ["the-batch"]

    def test_track_subscription_adds_source_once(self):
        daemon = make_daemon()
        source = object()

        daemon.track_subscription(source)
        daemon.track_subscription(source)

        assert daemon._subscriptions == [source]

    def test_clear_all_links_unsubscribes_from_tracked_sources_and_clears_subscribers(self):
        daemon = make_daemon()

        class FakeSource:
            def __init__(self):
                self.unsubscribed_from = []

            def unsubscribe(self, who):
                self.unsubscribed_from.append(who)

        source = FakeSource()
        daemon.track_subscription(source)
        daemon.subscribe(object())

        daemon.clear_all_links()

        assert source.unsubscribed_from == [daemon]
        assert daemon._subscriptions == []
        assert daemon.subscribers == []

    def test_clear_all_links_tolerates_sources_without_unsubscribe(self):
        daemon = make_daemon()
        daemon.track_subscription(object())  # no unsubscribe method
        daemon.clear_all_links()  # must not raise


class TestChildren:
    def test_register_child_adds_it_once(self):
        daemon = make_daemon()
        child = object()

        daemon.register_child(child)
        daemon.register_child(child)

        assert daemon._children == [child]

    def test_register_child_starts_it_immediately_if_parent_is_already_running(self):
        daemon = make_daemon()
        daemon.enabled = True
        daemon.start()
        try:

            class Child:
                def __init__(self):
                    self.started = False

                def start(self):
                    self.started = True

            child = Child()
            daemon.register_child(child)

            assert child.started is True
        finally:
            daemon.stop()

    def test_unregister_child_stops_and_removes_it(self):
        daemon = make_daemon()

        class Child:
            def __init__(self):
                self.stopped_with_timeout = None

            def stop(self, timeout=5.0):
                self.stopped_with_timeout = timeout

        child = Child()
        daemon.register_child(child)

        daemon.unregister_child(child)

        assert child.stopped_with_timeout is not None
        assert daemon._children == []

    def test_stop_stops_all_registered_children(self):
        daemon = make_daemon()
        daemon.enabled = True

        class Child:
            def __init__(self):
                self.started = False
                self.stopped = False

            def start(self):
                self.started = True

            def stop(self, timeout=5.0):
                self.stopped = True

        child = Child()
        daemon.register_child(child)

        daemon.start()
        daemon.stop()

        assert child.stopped is True


class TestUpdateFields:
    def test_update_fields_updates_only_the_requested_fields_and_reports_change(self):
        daemon = make_daemon()
        daemon.foo = "old"
        daemon.bar = "old"

        changed = daemon.update_fields({"foo": "new", "bar": "old", "baz": "ignored"}, ["foo", "bar"])

        assert daemon.foo == "new"
        assert daemon.bar == "old"
        assert changed is True

    def test_update_fields_reports_no_change_when_values_are_identical(self):
        daemon = make_daemon()
        daemon.foo = "same"

        changed = daemon.update_fields({"foo": "same"}, ["foo"])

        assert changed is False


class TestNewDaemon:
    def test_new_daemon_returns_id_and_config(self):
        id_, conf = BaseDaemon.new_daemon("my-name", "some_kind")

        assert conf == {"id": id_, "enabled": True, "type": "some_kind", "name": "my-name"}


class TestRepr:
    def test_repr_includes_class_name_and_enabled_state(self):
        daemon = make_daemon()
        daemon.enabled = True

        text = repr(daemon)

        assert "BaseDaemon" in text
        assert "enabled=True" in text
        assert str(daemon) == text


class TestApplyConfigLoggingLifecycle:
    def test_enabling_logging_builds_starts_and_subscribes_the_file_logger(self):
        file_logger = FakeFileLogger()
        daemon = make_daemon_with_shared(file_logger)

        daemon.apply_config({"logging": {"enabled": True, "processor": {"type": "log_row"}}})

        assert daemon.file_logger is file_logger
        assert file_logger.started is True
        assert file_logger in daemon.subscribers

    def test_disabling_logging_stops_and_clears_the_existing_file_logger(self):
        file_logger = FakeFileLogger()
        daemon = make_daemon_with_shared(file_logger)
        daemon.apply_config({"logging": {"enabled": True, "processor": {"type": "log_row"}}})

        daemon.apply_config({"logging": {"enabled": False}})

        assert file_logger.stopped is True
        assert file_logger.cleared is True
        assert daemon.file_logger is None
        assert daemon.shared.registry.file_manager.removed == [file_logger]

    def test_no_logging_key_leaves_file_logger_untouched(self):
        daemon = make_daemon_with_shared()
        daemon.apply_config({})
        assert daemon.file_logger is None

    def test_configured_flag_set_after_first_apply_and_restart_flagged_on_later_changes(self):
        daemon = make_daemon()
        daemon.enabled = False

        daemon.apply_config({})
        assert daemon._configured is True
        assert daemon.thread_needs_restart is False

        daemon.apply_config({"enabled": True})
        assert daemon.thread_needs_restart is True
