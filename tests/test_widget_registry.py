# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui import widget_registry
from blinkview.ui.constants import WidgetName
from blinkview.ui.widget_registry import build_widget_factory_map, register_widget_factory


class FakeWidgetA:
    pass


class FakeWidgetB:
    pass


@pytest.fixture
def isolated_registrations(monkeypatch):
    """Swaps in an empty _WIDGET_REGISTRATIONS list so fake registrations in these tests don't
    leak into (or collide with) the real, module-import-time ones."""
    monkeypatch.setattr(widget_registry, "_WIDGET_REGISTRATIONS", [])


def test_register_widget_factory_appends_without_raising(isolated_registrations):
    register_widget_factory("Widget")(FakeWidgetA)

    assert widget_registry._WIDGET_REGISTRATIONS == [("Widget", FakeWidgetA)]


def test_register_widget_factory_returns_the_class_unchanged(isolated_registrations):
    decorated = register_widget_factory("Widget")(FakeWidgetA)

    assert decorated is FakeWidgetA


def test_build_widget_factory_map_resolves_registrations(isolated_registrations):
    register_widget_factory("Widget")(FakeWidgetA)
    register_widget_factory("Gadget")(FakeWidgetB)

    widget_factories = build_widget_factory_map()

    assert widget_factories == {"Widget": FakeWidgetA, "Gadget": FakeWidgetB}


def test_build_widget_factory_map_raises_on_duplicate_name(isolated_registrations):
    register_widget_factory("Widget")(FakeWidgetA)
    register_widget_factory("Widget")(FakeWidgetB)

    with pytest.raises(ValueError):
        build_widget_factory_map()


def test_real_registrations_spot_check_across_widgets():
    """Spot-checks the real, undisturbed _WIDGET_REGISTRATIONS list (populated by every
    @register_widget_factory call fired at import time) resolves to the exact real widget class -
    not just that the helper works against an isolated fake list."""
    from blinkview.ui.widgets.config.dynamic_config import DynamicConfigWidget
    from blinkview.ui.widgets.log_table_viewer import LogTableViewerWidget
    from blinkview.ui.widgets.log_viewer import LogViewerWidget
    from blinkview.ui.widgets.plotter import TelemetryPlotter
    from blinkview.ui.widgets.telemetry_table import TelemetryTable
    from blinkview.ui.widgets.TelemetryWatch import TelemetryWatch
    from blinkview.ui.widgets.update_widget import UpdateWidget

    widget_factories = build_widget_factory_map()

    assert widget_factories[WidgetName.LOG_VIEWER] is LogViewerWidget
    assert widget_factories[WidgetName.LOG_TABLE_VIEWER] is LogTableViewerWidget
    assert widget_factories[WidgetName.TELEMETRY_TABLE] is TelemetryTable
    assert widget_factories[WidgetName.DYNAMIC_CONFIG] is DynamicConfigWidget
    assert widget_factories[WidgetName.TELEMETRY_PLOTTER] is TelemetryPlotter
    assert widget_factories[WidgetName.TELEMETRY_WATCH] is TelemetryWatch
    assert widget_factories[WidgetName.UPDATE_WIDGET] is UpdateWidget
