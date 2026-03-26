# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Thread, Event, Lock
from typing import List, Iterable
from types import SimpleNamespace

from blinkview.core.BaseBindableConfigurable import BaseBindableConfigurable
from blinkview.core.base_configurable import configuration_property
from blinkview.core.constants import SysCat
from blinkview.utils.generate_id import generate_id
from blinkview.utils.settings_updater import update_object_from_config


@configuration_property("enabled", type="boolean", default=False, required=True, description="Whether this daemon is enabled and should run.")
@configuration_property("logging", title="Log incoming data", type="object", hidden=True, _factory="file_logging", _factory_default="default",)
class BaseDaemon(BaseBindableConfigurable):
    def __init__(self):
        super().__init__()

        self.enabled = False
        self._configured = False  # Tracks if initial config has been applied at least once

        # New Thread Lifecycle properties
        self._thread = None
        self._stop_event = Event()

        self.thread_needs_restart = False  # Flag to indicate if a restart is needed after config changes

        self._subscribers_lock = Lock()
        self.subscribers: list = []
        self._subscriptions: list = []

        self.targets: List[SysCat] = []
        self.sources: List[SysCat] = []

        self.file_logger = None  # Optional file logger instance for binary logging

    @property
    def is_running(self) -> bool:
        """The absolute source of truth for thread state."""
        return self._thread is not None and self._thread.is_alive()

    def apply_config(self, config: dict):
        if self.logger:
            self.logger.info(f"{self.__class__.__name__}: Applying config: {config}")

        changed = super().apply_config(config)

        try:
            logging_cfg = config.get("logging")
            if logging_cfg is not None:
                logging_cfg["enabled"] = self.enabled
                logging_cfg["name"] = self.name

                if self.logger:
                    self.logger.info(f"Logging conf: {logging_cfg}")
                if logging_cfg.get("enabled"):
                    if self.file_logger is None:
                        ns = SimpleNamespace(get_logger=self.shared.registry.logger_creator("file_logging", self.local.logging_id), logging_id=self.local.logging_id)
                        self.file_logger = self.shared.factories.build("file_logging", logging_cfg, self.shared, ns)
                        self.file_logger.start()
                    self.subscribe(self.file_logger)
        except Exception as e:
            if self.logger:
                self.logger.error(f"{self.__class__.__name__}: Failed to apply logging.", e)

        if changed and self._configured:
            self.thread_needs_restart = True

        self._configured = True

        return changed

    def start(self):
        if not self.enabled:
            if self.logger: self.logger.info("[DAEMON] Not enabled, skipping start.")
            return

        if self.is_running:
            if self.logger: self.logger.info("[DAEMON] Already running, skipping start.")
            return

        if self.logger: self.logger.info("[DAEMON] Starting...")

        self._stop_event.clear()
        self._thread = Thread(target=self._run_wrapper, daemon=True)
        self._thread.start()

    def stop(self, timeout=5.0):
        if not self.is_running:
            return

        if self.logger: self.logger.info("[DAEMON] Stopping...")
        self._stop_event.set()  # Signals the loop to exit

        if self._thread:
            self._thread.join(timeout)
            if self._thread.is_alive():
                if self.logger: self.logger.warn(f"[DAEMON] Did not stop within {timeout} seconds.")
            else:
                if self.logger: self.logger.info("[DAEMON] Stopped cleanly.")
            self._thread = None  # Clean up the reference

    def restart(self):
        self.thread_needs_restart = False
        if self.logger: self.logger.info("[DAEMON] Restarting...")
        if self.is_running:
            self.stop()
        self.start()

    def _run_wrapper(self):
        """Wraps the run method to ensure graceful exit logging."""
        try:
            self.run()
        except Exception as e:
            if self.logger: self.logger.exception(f"[DAEMON] Crashed during run.", e)

    def run(self):
        """Override this in subclasses. Use `while not self._stop_event.is_set():`"""
        pass

    def subscribe(self, subscriber):
        with self._subscribers_lock:
            if subscriber not in self.subscribers:
                self.subscribers.append(subscriber)
                if hasattr(subscriber, "track_subscription"):
                    subscriber.track_subscription(self)  # Track the source for cleanup

    def unsubscribe(self, subscriber):
        with self._subscribers_lock:
            if subscriber in self.subscribers:
                self.subscribers.remove(subscriber)

    def distribute(self, batch: list):
        # FIX: Copy the list while locked, then release the lock immediately!
        with self._subscribers_lock:
            subs_copy = list(self.subscribers)

        # Distribute without holding the lock
        for subscriber in subs_copy:
            try:
                subscriber.put(batch)
            except Exception as e:
                if self.logger:
                    self.logger.error("Queue delivery failed.", e)

    def update_fields(self, config: dict, fields: Iterable[str]) -> bool:
        return update_object_from_config(self, config, fields)

    def track_subscription(self, source_obj):
        """Adds an upstream source to the tracking list."""
        with self._subscribers_lock:
            if source_obj not in self._subscriptions:
                self._subscriptions.append(source_obj)

    def clear_all_links(self):
        """Standardized cleanup for stopping the daemon."""
        with self._subscribers_lock:
            for source in self._subscriptions:
                # Assuming your sources have a standard disconnect/unsubscribe method
                try:
                    source.unsubscribe(self)
                except AttributeError:
                    pass
            self._subscriptions.clear()

        with self._subscribers_lock:
            self.subscribers.clear()

    @classmethod
    def new_daemon(cls, name, kind, enabled=True, prefix=None, parent: dict = None):
        parent = parent or {}
        id_ = generate_id(prefix, list(parent.keys()))
        conf = {
            "id": id_,
            "enabled": True,
            "type": kind,
            "name": name
        }
        return id_, conf
