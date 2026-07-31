# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import sleep

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configuration_property


@configuration_property(
    "enabled",
    type="boolean",
    default=False,
    description="When enabled, monitors the working directory for a flash lock file, closing the serial port to allow external flashing.",
)
@configuration_property(
    "identifier",
    type="string",
    default="uart",
    description="The identifier string used for the handshake files (e.g., if set to 'esp32', files will be '.bv_source_esp32.lock').",
)
@bindable
class SourceHandshakeManager:
    """Manages external flashing synchronization via file system locks."""

    enabled: bool
    identifier: str

    def __init__(self):
        super().__init__()

        self.is_active = False
        self._lock_ts = 0
        self._task_id = None

        # Cached Path objects
        self._lock_path: Path = None
        self._closed_path: Path = None
        self._update_cached_paths()

    def _update_cached_paths(self, ident: str = None):
        """Regenerates the cached Path objects when the identifier changes."""
        safe_ident = ident or getattr(self, "identifier", "uart")
        self._lock_path = Path(f".bv_source_{safe_ident}.lock")
        self._closed_path = Path(f".bv_source_{safe_ident}.closed")

    def apply_config(self, config: dict):
        """Native framework override to handle lifecycle changes when config updates."""
        # 1. Capture old identifier in case it changed so we can clean up old files
        old_ident = getattr(self, "identifier", "uart")

        # 2. Let the framework apply the new properties
        super().apply_config(config)

        # 3. Re-cache the Path objects using the newly applied identifier
        self._update_cached_paths(self.identifier)

        # 4. Handle state transitions
        if old_ident != self.identifier or not self.enabled:
            # Clean up files using the old identifier before we fully transition
            self._clear_specific_files(old_ident)

        if not self.enabled:
            self.stop()
        else:
            self.clear_files()  # Ensure pristine slate using new identifier

    def start(self):
        """Starts the periodic monitoring task if not already running."""
        if self._task_id is None and self.enabled:
            self._task_id = self.shared.tasks.run_periodic(0.1, self._check_loop)

    def stop(self):
        """Stops the periodic task and cleans up artifacts."""
        if self._task_id is not None:
            try:
                self.logger.info("Stopping flash handshake periodic task.")
                self.shared.tasks.stop_periodic(self._task_id)
            except Exception as e:
                self.logger.error("Failed to cleanly stop flash handshake task: %s", e)
            finally:
                self._task_id = None
        self.clear_files()

    def clear_files(self):
        """Removes local handshake files using the currently cached paths."""
        self._clear_paths(self._lock_path, self._closed_path)
        self.is_active = False

    def _clear_specific_files(self, ident: str):
        """Helper to clear files for a specific identifier."""
        # If the ident matches the current one, reuse cached paths to avoid instantiation
        if ident == self.identifier:
            self.clear_files()
            return

        lock_path = Path(f".bv_source_{ident}.lock")
        closed_path = Path(f".bv_source_{ident}.closed")
        self._clear_paths(lock_path, closed_path)
        self.is_active = False

    def _clear_paths(self, *paths: Path):
        """Safely unlinks a list of Path objects."""
        for file_path in paths:
            if file_path.exists():
                try:
                    self.logger.info("Removing handshake file: %s", file_path)
                    file_path.unlink()
                except Exception as e:
                    self.logger.error("Failed to remove handshake file '%s': %s", file_path, e)

    def _check_loop(self):
        """The core periodic loop monitoring the file system."""
        lock_exists = self._lock_path.exists()
        current_time_ns = self.shared.time_ns()

        if self.is_active and lock_exists:
            if (current_time_ns - self._lock_ts) > 60_000_000_000:
                self.logger.warning("Flash handshake timed out after 60s! Forcing environment reset.")
                self.clear_files()
                return

        if lock_exists and not self.is_active:
            self.logger.warning("Flash lock '%s' detected! Requesting serial loop release...", self._lock_path)
            self.is_active = True
            self._lock_ts = current_time_ns

            # Note: Ensure self.is_port_closed_cb is set/injected elsewhere in your class!
            while not self.is_port_closed_cb():
                sleep(0.02)

            try:
                self._closed_path.write_text("closed")
            except Exception as e:
                self.logger.error("Failed to create handshake file %s: %s", self._closed_path, e)

        elif not lock_exists and self.is_active:
            self.logger.info("Flash lock removed. Restoring serial connection availability.")
            self.is_active = False
            if self._closed_path.exists():
                try:
                    self._closed_path.unlink()
                except Exception:
                    pass
