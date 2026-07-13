import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("QT_API", "pyside6")

# Pre-warms the core.device_identity/core.id_registry import cluster in a safe order before any
# test module gets a chance to import a blinkview.ui.widgets.* module as its very first touch of
# that cluster - which trips a pre-existing circular import (core.device_identity <->
# core.id_registry.registry) that only avoids tripping when something else imports
# core.id_registry first. blinkview.ui.main_window (the real app entry point) imports
# core.registry before any widget module for the same reason, which is why it never hits this.
import blinkview.core.registry  # noqa: E402, F401

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def qapp():
    from qtpy.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app
