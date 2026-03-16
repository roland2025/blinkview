# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import requests
import threading
from datetime import datetime, timedelta, timezone
from packaging import version
from pathlib import Path

# Dynamic version import
from blinkview import __version__ as CURRENT_VERSION
from blinkview.utils.global_settings import GlobalSettings


class GitHubUpdate:
    REPO_API_URL = "https://api.github.com/repos/roland2025/blinkview/releases"
    REPO_RELEASES_URL = "https://github.com/roland2025/blinkview/releases"

    @classmethod
    def check_async(cls, force=False, callback=None):
        """
        Fire-and-forget background check.
        Optional callback: func(has_update, latest_version)
        """
        thread = threading.Thread(
            target=cls._threaded_check,
            args=(force, callback),
            daemon=True
        )
        thread.start()

    @classmethod
    def _threaded_check(cls, force, callback):
        has_update, latest_v = cls.check(force=force)
        if callback:
            callback(has_update, latest_v)

    @classmethod
    def check(cls, force=False, include_pre=True):
        """
        include_pre: If True, looks at the very latest tag (stable or dev).
                     If False, finds the latest stable release in the list.
        """
        settings = GlobalSettings()
        check_data = settings.get("update_check", {})

        if not force and not cls._should_check(check_data):
            cached_v = check_data.get("latest_version")
            return cls._is_newer(cached_v), cached_v

        try:
            # 1. Fetch the list of releases (GitHub returns them sorted by date)
            response = requests.get(cls.REPO_API_URL, timeout=5)
            response.raise_for_status()
            releases = response.json()

            if not releases:
                return False, None

            # 2. Pick the target release
            if include_pre:
                # The first one in the list is always the newest tag
                target_release = releases[0]
            else:
                # Find the first release that ISN'T a pre-release
                target_release = next((r for r in releases if not r.get("prerelease")), releases[0])

            latest_v = target_release.get("tag_name", "v0.0.0").strip('v')

            # 3. Update Global Settings
            settings["update_check"] = {
                "last_checked": datetime.now(timezone.utc).isoformat() + "Z",
                "latest_version": latest_v,
                "is_prerelease": target_release.get("prerelease", False)
            }
            settings.save()

            return cls._is_newer(latest_v), latest_v

        except Exception as e:
            return False, check_data.get("latest_version")

    @staticmethod
    def _is_newer(latest_v):
        if not latest_v:
            return False
        try:
            return version.parse(latest_v) > version.parse(CURRENT_VERSION)
        except:
            return False

    @staticmethod
    def _should_check(data):
        last_str = data.get("last_checked")
        if not last_str:
            return True

        try:
            last_time = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) > last_time + timedelta(days=1)
        except:
            return True

    @classmethod
    def get_update_message(cls):
        """Returns a string if an update is available, else None."""
        from blinkview import __version__
        from blinkview.utils.global_settings import GlobalSettings
        from packaging import version

        upd = GlobalSettings().get("update_check", {})
        latest = upd.get("latest_version")

        if latest and version.parse(latest) > version.parse(__version__):
            return (
                f"🚀 New version available: v{latest} (Current: v{__version__})\n"
                f"   Run 'git pull' to update your local repository."
            )
        return None
