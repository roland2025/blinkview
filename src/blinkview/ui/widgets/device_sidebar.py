# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.io.BaseReader import BaseReader
from blinkview.ui.widgets.base_list_item import BaseListItemWidget
from blinkview.ui.widgets.base_sidebar_widget import BaseSidebarWidget


class DeviceListItemWidget(BaseListItemWidget):
    """Basic implementation for standard devices."""

    pass


class DeviceSidebarWidget(BaseSidebarWidget):
    def __init__(self, config_node, gui_context):
        super().__init__(
            config_node=config_node,
            gui_context=gui_context,
            toolbar_title="Device Actions",
            add_btn_text="➕ Add Source",
            factory_key="source",
            input_title="Source Name",
            item_name_prefix="Source",
            list_item_class=DeviceListItemWidget,  # Your existing widget class
        )

    def generate_daemon_config(self, name: str, item_type: str, parent_config: dict):
        return BaseReader.new_daemon(name, item_type, prefix="src", parent=parent_config)
