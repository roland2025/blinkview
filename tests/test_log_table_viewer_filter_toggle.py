# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The module-filter sidebar's "Enable Filter" toggle, on a real LogTableViewerWidget: the
refetch comes from the sidebar's own filter_changed signal (set_enabled emits it), so each
toggle must refetch exactly once - not zero (stale rows) and not twice (a second, direct
connection to the toggle re-running the same full scan)."""

import pytest

from blinkview.ui.widgets.log_table_viewer import LogTableViewerWidget
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "log_table_viewer_filter_toggle_test")
    yield reg
    reg.stop()


@pytest.fixture
def viewer(qapp, qtbot, registry):
    w = LogTableViewerWidget(make_real_gui_context(registry))
    qtbot.addWidget(w)
    return w


def _count_refetches(viewer):
    calls = []
    original = viewer.model.reload_and_redraw

    def spy():
        calls.append(viewer.filter_sidebar.log_filter.enabled)
        original()

    viewer.model.reload_and_redraw = spy
    return calls


@pytest.mark.parametrize("enable", [True, False])
def test_enable_toggle_refetches_once(viewer, enable):
    viewer.filter_sidebar.action_enable.setChecked(not enable)
    calls = _count_refetches(viewer)

    viewer.filter_sidebar.action_enable.setChecked(enable)

    assert calls == [enable]  # exactly one refetch, made after the filter's state flipped
