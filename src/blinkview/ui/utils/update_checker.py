# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def check_for_updates_silently(gui_context):
    from packaging.version import parse as parse_version

    from blinkview import __version__
    from blinkview.ui.widgets import toast_dispatcher
    from blinkview.ui.widgets.toast import ToastType
    from blinkview.utils.updater import Updater

    # Capture the parent reference before entering the background thread
    # Usually you want to anchor to the main window
    main_window = gui_context.registry.viewer

    def _bg_worker():
        try:
            updater = Updater(gui_context.settings)
            updater.fetch(force=False)
            latest = updater.get_latest_version()

            if latest and parse_version(latest) > parse_version(__version__):
                toast_dispatcher.notify(
                    message=f"BlinkView <b>{latest}</b> is available.",
                    toast_type=ToastType.INFO,
                    action_text="UPDATER",
                    action_callback=lambda: gui_context.create_widget("UpdateWidget", "Updates", as_window=True),
                    parent=main_window,  # Anchor specifically to the viewer window
                )
        except Exception:
            pass

    gui_context.registry.system_ctx.tasks.run_task(_bg_worker)
