# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


class WidgetName:
    """Widget class names used as BlinkMainWindow.widget_factories keys - see
    ui/widget_registry.py for the decorator that registers a widget class under one of these."""

    LOG_VIEWER = "LogViewerWidget"
    LOG_TABLE_VIEWER = "LogTableViewerWidget"
    TELEMETRY_TABLE = "TelemetryTable"
    DYNAMIC_CONFIG = "DynamicConfigWidget"
    TELEMETRY_PLOTTER = "TelemetryPlotter"
    TELEMETRY_WATCH = "TelemetryWatch"
    UPDATE_WIDGET = "UpdateWidget"
