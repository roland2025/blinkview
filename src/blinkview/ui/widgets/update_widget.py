# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from datetime import date, datetime
from pathlib import Path

from PySide6.QtCore import QObject, QTimer, Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from blinkview import __version__
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.update_checker import check_post_update
from blinkview.ui.widgets.message_box import MessageBox
from blinkview.utils.updater import UpdateError, Updater


class TaskSignals(QObject):
    """Bridge to relay fetch results from TaskManager to the Qt UI Thread."""

    fetch_completed = Signal(list)
    error_occurred = Signal(str)


class UpdateWidget(QWidget):
    def __init__(self, gui_context, _=None, parent=None):
        super().__init__(parent)
        self.gui_context: GUIContext = gui_context
        self.task_manager = gui_context.registry.system_ctx.tasks
        self.updater = None

        self.signals = TaskSignals()
        self.signals.fetch_completed.connect(self._on_fetch_finished)
        self.signals.error_occurred.connect(self._on_error)

        self.tab_name = "Updater"
        self._setup_ui()

        if self.ensure_updater():
            # Show local cache immediately

            QTimer.singleShot(0, lambda: check_post_update(self.updater, parent=self))
            QTimer.singleShot(0, self.list_local_versions)
            self.update_status()

            # Always attempt an auto-fetch; the Updater will skip it if
            # the cooldown hasn't expired internally.
            QTimer.singleShot(100, lambda: self.request_fetch(is_auto=True))

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        self.status_label = QLabel("<b>Status:</b> Initializing...")
        layout.addWidget(self.status_label)

        # --- Channel Selector ---
        channel_layout = QHBoxLayout()
        channel_layout.addWidget(QLabel("Update Channel:"))

        self.channel_combo = QComboBox()
        self.channel_combo.addItem("Stable", "stable")
        self.channel_combo.addItem("Release Candidate", "rc")
        self.channel_combo.addItem("Development", "dev")

        # Set the combo box to the currently saved setting
        current_channel = str(self.gui_context.settings.get("update.channel", "stable")).lower()
        idx = self.channel_combo.findData(current_channel)
        if idx >= 0:
            self.channel_combo.setCurrentIndex(idx)

        self.channel_combo.currentIndexChanged.connect(self._on_channel_changed)
        channel_layout.addWidget(self.channel_combo)
        channel_layout.addStretch()  # Push the combo box to the left
        layout.addLayout(channel_layout)
        # ------------------------

        layout.addWidget(QLabel("Available Versions:"))
        self.version_list = QListWidget()
        layout.addWidget(self.version_list)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()
        layout.addWidget(self.progress)

        btn_layout = QHBoxLayout()
        self.fetch_btn = QPushButton("Check for Updates")
        self.fetch_btn.clicked.connect(self.request_fetch)

        self.install_btn = QPushButton("Apply & Restart")
        self.install_btn.setEnabled(False)
        self.install_btn.clicked.connect(self.handle_install_request)

        # Added a path config button for convenience
        self.config_btn = QPushButton("Set Repo Path")
        self.config_btn.clicked.connect(self.prompt_for_path)

        btn_layout.addWidget(self.fetch_btn)
        btn_layout.addWidget(self.install_btn)
        btn_layout.addWidget(self.config_btn)
        layout.addLayout(btn_layout)

    def _on_channel_changed(self, index: int):
        """Triggered when the user changes the update channel dropdown."""
        selected_channel = self.channel_combo.itemData(index)

        # Save the new preference
        self.gui_context.settings.set("update.channel", selected_channel, scope="global")

        if self.updater:
            # Update the active updater instance so we don't need to recreate it
            self.updater.channel = selected_channel

            # Instantly re-filter and display the local tags
            self.list_local_versions()

    def list_local_versions(self):
        """Populates the list with tags already present in the local repository."""
        if not self.updater:
            return

        self.version_list.clear()
        try:
            versions = self.updater.get_versions(remote=False)
            if not versions:
                self.version_list.addItem("No local versions found. Please 'Fetch'.")
                self.install_btn.setEnabled(False)
                return

            for i, v in enumerate(versions):
                item_text = f"{v} (Local)" if i == 0 else v
                self.version_list.addItem(item_text)

            self.version_list.setCurrentRow(0)
            self.install_btn.setEnabled(self.gui_context.set_update_version is not None)
        except Exception as e:
            self.version_list.addItem(f"Error reading local tags: {e}")

    def ensure_updater(self) -> bool:
        """
        Attempts to initialize the updater.
        If path is missing, prompts the user.
        """
        try:
            self.updater = Updater(self.gui_context.settings)
            self.status_label.setText(f"<b>Current Version:</b> v{__version__}")
            self.fetch_btn.setEnabled(True)
            self.config_btn.setVisible(False)  # Hide the config button if the updater initializes successfully
            return True
        except UpdateError:
            # This happens if update.path is not set
            self.status_label.setText("<b>Status:</b> <span style='color:red;'>Update path not configured.</span>")
            self.fetch_btn.setEnabled(False)
            return self.prompt_for_path()

    def prompt_for_path(self) -> bool:
        """Opens a folder dialog and validates the selection using the static method."""
        current_path = self.gui_context.settings.get("update.path", "")
        selected_path = QFileDialog.getExistingDirectory(self, "Select BlinkView Source Repository", current_path)

        if not selected_path:
            return False

        selected_path = Path(selected_path).resolve()

        # Use the STATIC method to validate before doing anything else
        if not Updater.is_valid_repo(selected_path):
            MessageBox.warning(
                self,
                "Invalid Repository",
                "The selected folder is not a valid BlinkView source tree.\n"
                "Expected to find .git, pyproject.toml, and the blinkview source.",
            )
            return False

        # If valid, save and re-init
        self.gui_context.settings.set("update.path", str(selected_path), scope="global")

        try:
            self.updater = Updater(self.gui_context.settings)

            self.fetch_btn.setEnabled(True)
            self.request_fetch()  # Automatically fetch after setting a valid path
            return True
        except UpdateError as e:
            MessageBox.critical(self, "Initialization Error", str(e))
            return False

    def request_fetch(self, is_auto=False):
        """Background task to get tags. Updater handles internal cooldowns."""
        if not self.updater:
            if not self.ensure_updater():
                return

        self._set_loading(True)

        def _fetch_logic():
            try:
                # The 'force' parameter is determined by whether the user
                # manually clicked the button (not auto)
                self.updater.fetch(force=not is_auto)
                versions = self.updater.get_versions()
                self.signals.fetch_completed.emit(versions)
            except UpdateError as e:
                self.signals.error_occurred.emit(str(e))

        self.task_manager.run_task(_fetch_logic)

    def _set_loading(self, is_loading: bool):
        self.fetch_btn.setEnabled(not is_loading)
        self.install_btn.setEnabled(not is_loading if not is_loading else False)
        self.progress.setVisible(is_loading)

    def handle_install_request(self):
        """Validates selection, asks for confirmation, registers, and exits."""
        selected = self.version_list.currentItem()
        if not selected:
            return

        # Clean the version string (removes " (Latest)")
        version = selected.text().split(" ")[0]

        msg = (
            f"BlinkView will now register version <b>{version}</b> for installation "
            "and close immediately to complete the process.\n\n"
            "Do you want to proceed?"
        )

        confirm = MessageBox.question(
            self,
            "Confirm Update",
            msg,
        )

        if confirm == MessageBox.Btn.Yes:
            try:
                # Execute the registration callback (e.g., writing to a config or starting a shim)
                self.gui_context.set_update_version(version)

                # Hard exit to release file locks for the installer
                print(f"Update registered for {version}. Shutting down.")
            except Exception as e:
                MessageBox.critical(self, "Registration Error", f"Failed to register update: {e}")

    def update_status(self):
        last_time = self.gui_context.settings.get("update.last_fetch_time", 0)

        if last_time == 0:
            display_time = "Never"
        else:
            # Convert timestamp to a datetime object
            dt = datetime.fromtimestamp(last_time)
            today = date.today()
            delta = today - dt.date()

            if delta.days == 0:
                # It was today
                display_time = dt.strftime("%H:%M")
            elif delta.days == 1:
                # It was yesterday
                display_time = f"Yesterday {dt.strftime('%H:%M')}"
            elif delta.days < 7:
                # Within the last week: "Mon 14:30"
                display_time = dt.strftime("%a %H:%M")
            else:
                # Further back: "Mar 25"
                display_time = dt.strftime("%b %d")

        self.status_label.setText(f"<b>Version:</b> v{__version__} <small>(Checked: {display_time})</small>")

    def _on_fetch_finished(self, versions):
        self.update_status()
        self._set_loading(False)
        self.version_list.clear()
        if not versions:
            self.version_list.addItem("No versions found.")
            return

        for i, v in enumerate(versions):
            item_text = f"{v} (Latest)" if i == 0 else v
            self.version_list.addItem(item_text)

        self.version_list.setCurrentRow(0)
        self.install_btn.setEnabled(self.gui_context.set_update_version is not None)

    def _on_error(self, error_msg):
        self._set_loading(False)
        MessageBox.critical(self, "Update Error", error_msg)
