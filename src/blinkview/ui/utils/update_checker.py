# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


def check_for_updates_silently(gui_context, parent=None):
    from packaging.version import parse as parse_version

    from blinkview import __version__
    from blinkview.ui.widgets.toast import ToastType
    from blinkview.ui.widgets.toast_dispatcher import toast_dispatcher
    from blinkview.utils.updater import Updater

    # Capture the parent reference before entering the background thread
    # Usually you want to anchor to the main window
    updater = Updater(gui_context.settings)
    check_post_update(updater, parent)

    def _bg_worker():
        try:
            print(f"checking for updates...")
            updater.fetch(force=False)
            latest = updater.get_latest_version()

            if latest and parse_version(latest) > parse_version(__version__):
                print(f"{latest} is available")
                toast_dispatcher.notify(
                    message=f"BlinkView <b>{latest}</b> is available.",
                    toast_type=ToastType.INFO,
                    duration=30,
                    action_text="SHOW",
                    action_callback=lambda: gui_context.create_widget(
                        "UpdateWidget", "Updates", as_window=True, reattach_on_close=False
                    ),
                    parent=parent,  # Anchor specifically to the viewer window
                )
        except Exception:
            import traceback

            print(traceback.format_exc())
            pass

    gui_context.registry.system_ctx.tasks.run_task(_bg_worker)


def check_post_update(updater, parent=None):
    from blinkview import __version__
    from blinkview.ui.widgets.toast import ToastType
    from blinkview.ui.widgets.toast_dispatcher import toast_dispatcher

    success, target_version = updater.check_version_status(__version__)

    if success is True:
        toast_dispatcher.notify(
            f"BlinkView was successfully updated to <b>{__version__}</b>.", ToastType.SUCCESS, parent=parent
        )
    elif success is False:
        toast_dispatcher.notify(
            f"Upgrade to {target_version} failed.\nYou are still running v{__version__}.",
            ToastType.ERROR,
            parent=parent,
        )
