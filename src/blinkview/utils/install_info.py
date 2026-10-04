# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""How this BlinkView was installed, so updates and caches know where to go.

Two independent questions:

- Where do updates come from? A local source checkout (``uv tool install .`` / editable) updates
  through git tags in that checkout; anything else (an index install, a wheel file, a git URL)
  updates from the package index.
- Which installer owns the environment? ``uv tool`` leaves a ``uv-receipt.toml`` in the tool's
  venv; anything else is treated as plain pip (pipx included, its venvs run ``python -m pip``
  fine).

No Qt and no numba here - this is read on the CLI path and before the Numba cache is exported.
"""

import json
import re
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from urllib.parse import unquote, urlparse

DIST_NAME = "blinkview"


class Installer(str, Enum):
    UV_TOOL = "uv_tool"
    PIP = "pip"


class UpdateSource(str, Enum):
    GIT = "git"  # tags in a local source checkout (update.path)
    PYPI = "pypi"  # the package index


@dataclass(frozen=True)
class InstallInfo:
    installer: Installer
    # The local checkout this was installed from (PEP 610 dir_info), None for any other install.
    source_dir: Path | None
    editable: bool
    # Extras recorded by the installer, e.g. ("all",); empty when unknown.
    extras: tuple[str, ...]

    @property
    def from_source(self) -> bool:
        return self.source_dir is not None


_FROM_METADATA = object()


def detect_install(prefix: Path | str | None = None, direct_url_text=_FROM_METADATA) -> InstallInfo:
    """Inspects the running environment. Both arguments exist for tests (direct_url_text=None
    meaning "no direct_url.json"); by default they come from sys.prefix and blinkview's installed
    dist-info."""
    prefix = Path(prefix if prefix is not None else sys.prefix)
    if direct_url_text is _FROM_METADATA:
        direct_url_text = _read_direct_url()

    source_dir, editable = _parse_direct_url(direct_url_text)

    receipt = prefix / "uv-receipt.toml"
    if receipt.is_file():
        installer = Installer.UV_TOOL
        extras = _receipt_extras(receipt.read_text(encoding="utf-8"))
    else:
        installer = Installer.PIP
        extras = ()

    return InstallInfo(installer=installer, source_dir=source_dir, editable=editable, extras=extras)


def resolve_update_source(settings, info: InstallInfo | None = None) -> UpdateSource:
    """update.source = auto | git | pypi; auto (the default) follows how this was installed."""
    choice = str(settings.get("update.source", "auto") or "auto").lower()
    if choice == UpdateSource.GIT.value:
        return UpdateSource.GIT
    if choice == UpdateSource.PYPI.value:
        return UpdateSource.PYPI

    info = info if info is not None else detect_install()
    return UpdateSource.GIT if info.from_source else UpdateSource.PYPI


def _read_direct_url() -> str | None:
    """First direct_url.json among all blinkview distributions on sys.path, not just the first
    distribution: a stale src/blinkview.egg-info from an old setuptools build shadows the real
    dist-info whenever src/ comes first on the path (pytest's pythonpath does that)."""
    from importlib.metadata import distributions

    for dist in distributions(name=DIST_NAME):
        text = dist.read_text("direct_url.json")
        if text:
            return text
    return None


def _parse_direct_url(text: str | None) -> tuple[Path | None, bool]:
    """PEP 610: only dir_info is a local checkout. archive_info (a wheel/sdist file) and vcs_info
    (a git URL) are snapshots with no repo to fetch tags in, so they update from the index."""
    if not text:
        return None, False
    try:
        data = json.loads(text)
    except ValueError:
        return None, False

    dir_info = data.get("dir_info")
    if dir_info is None:
        return None, False

    path = _file_url_to_path(data.get("url", ""))
    return path, bool(dir_info.get("editable", False))


def _file_url_to_path(url: str) -> Path | None:
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return None
    path = unquote(parsed.path)
    # file:///C:/x -> "/C:/x" on Windows
    if re.match(r"^/[A-Za-z]:", path):
        path = path[1:]
    return Path(path)


def _receipt_extras(text: str) -> tuple[str, ...]:
    import tomllib

    try:
        requirements = tomllib.loads(text).get("tool", {}).get("requirements", [])
    except tomllib.TOMLDecodeError:
        return ()
    for req in requirements:
        if str(req.get("name", "")).lower() == DIST_NAME:
            return tuple(req.get("extras", ()))
    return ()
