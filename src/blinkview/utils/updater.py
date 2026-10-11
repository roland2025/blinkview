# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import sys
from pathlib import Path, PureWindowsPath
from time import time
from typing import Callable, NamedTuple

from packaging.version import InvalidVersion
from packaging.version import parse as parse_version

from blinkview.core.settings_manager import SettingsManager
from blinkview.utils.install_info import Installer, InstallInfo, UpdateSource, detect_install, resolve_update_source

DEFAULT_INDEX_URL = "https://pypi.org/pypi"
PYPI_SIMPLE_URL = "https://pypi.org/simple"

# How long the detached Windows install waits for this process to exit before it tries anyway.
EXIT_WAIT_SECONDS = 60
# ...and how long it waits before its one retry of a failed install.
INSTALL_RETRY_DELAY_SECONDS = 3

# "Close and Continue": how long another instance's window gets to finish its own shutdown (it
# compresses its session on the way out), and how long a killed windowless one gets to disappear.
WINDOW_CLOSE_SECONDS = 60
KILL_WAIT_SECONDS = 5


class UpdateError(Exception):
    """Custom exception for update-related failures."""

    pass


def make_updater(settings: SettingsManager | None = None, info: InstallInfo | None = None) -> "Updater":
    """The updater matching how this BlinkView was installed (see install_info.resolve_update_source)."""
    settings = settings or SettingsManager()
    info = info if info is not None else detect_install()

    if resolve_update_source(settings, info) == UpdateSource.GIT:
        return GitUpdater(settings, info)
    return PyPIUpdater(settings, info)


def _describe_blink_command(cmdline: list[str]) -> str | None:
    """`python.exe <...>/blink.exe mcp` -> "blink mcp"; None for a process that isn't BlinkView
    (the environment's Python running something else)."""
    args = cmdline[1:]
    if args[:2] == ["-m", "blinkview"]:
        args = args[2:]
    elif args and PureWindowsPath(args[0]).stem.lower() == "blink":  # splits on / as well
        args = args[1:]
    else:
        return None
    return " ".join(["blink", *args])


class RunningInstance(NamedTuple):
    pid: int  # of the environment's python.exe, the middle of blink.exe -> python.exe -> python.exe
    command: str  # "blink mcp"

    def __str__(self) -> str:
        return f"{self.command} (pid {self.pid})"


def find_other_instances() -> list[RunningInstance]:
    """Other running BlinkView processes of this install - a second window, `blink mcp`. Each
    one keeps blink.exe (and whatever .pyds it loaded) locked on Windows, so an install next to
    it fails halfway. Found by their interpreter living in this environment; without psutil (an
    install with no GUI extra) nothing is found."""
    try:
        import psutil
    except ImportError:
        return []

    prefix = Path(sys.prefix)
    this_process = psutil.Process()
    # The launcher chain above us (blink.exe -> the venv's python.exe -> us) exits with us.
    own = {this_process.pid, *(p.pid for p in this_process.parents())}

    found = []
    for proc in psutil.process_iter(["pid", "exe", "cmdline"]):
        exe = proc.info["exe"]
        if proc.info["pid"] in own or not exe or not Path(exe).is_relative_to(prefix):
            continue
        command = _describe_blink_command(proc.info["cmdline"] or [])
        if command is not None:
            found.append(RunningInstance(proc.info["pid"], command))
    return found


def post_close_to_windows(pids: set[int]) -> set[int]:
    """Asks the visible top-level windows of these processes to close, as a click on their X
    does (WM_CLOSE): BlinkMainWindow.closeEvent then saves the layout and stops the registry
    before it exits. Returns the pids that had such a window. Windows only; nothing elsewhere."""
    if sys.platform != "win32":
        return set()

    import ctypes
    from ctypes import wintypes

    user32 = ctypes.windll.user32
    WM_CLOSE = 0x0010
    GW_OWNER = 4
    asked = set()

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def on_window(hwnd, _lparam):
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        # Unowned and visible: the main window and detached tab windows, not Qt's hidden
        # helper windows or a dialog (whose owner gets the message instead).
        if pid.value in pids and user32.IsWindowVisible(hwnd) and not user32.GetWindow(hwnd, GW_OWNER):
            user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)
            asked.add(pid.value)
        return True

    user32.EnumWindows(on_window, 0)
    return asked


def close_instances(
    instances: list[RunningInstance],
    window_timeout: float = WINDOW_CLOSE_SECONDS,
    on_wait: Callable[[], None] | None = None,
):
    """Ends each instance's whole chain: the interpreter found by find_other_instances(), the
    real one below it and the blink.exe launcher above it (the locked file).

    One with a window is asked to close and given window_timeout to finish its own shutdown;
    it is never killed, so one that doesn't make it is simply still running afterwards. One
    without a window (`blink mcp`, a headless `blink`) can't be asked from outside on Windows
    and is killed. on_wait is called a few times a second while waiting (the GUI's event loop)."""
    import psutil

    closing, doomed = [], []
    for instance in instances:
        try:
            proc = psutil.Process(instance.pid)
            chain = [*proc.children(recursive=True), proc]
            parent = proc.parent()
            if parent is not None and PureWindowsPath(parent.name()).stem.lower() == "blink":
                chain.append(parent)
        except psutil.Error:
            continue  # gone by itself since it was found
        if post_close_to_windows({p.pid for p in chain}):
            closing.extend(chain)
        else:
            doomed.extend(chain)

    for proc in doomed:
        try:
            proc.terminate()
        except psutil.Error:
            pass
    psutil.wait_procs(doomed, timeout=KILL_WAIT_SECONDS)

    deadline = time() + window_timeout
    while closing and time() < deadline:
        _gone, closing = psutil.wait_procs(closing, timeout=0.1)
        if on_wait is not None:
            on_wait()


class Updater:
    """Shared update behavior: channel filtering, fetch cooldown, pending-version bookkeeping and
    the detached reinstall. Subclasses supply where versions come from and the install command."""

    source: UpdateSource

    def __init__(self, settings: SettingsManager | None = None, info: InstallInfo | None = None):
        self.settings = settings or SettingsManager()
        self.info = info if info is not None else detect_install()

        # An explicit update.features wins; otherwise keep the extras the installer recorded.
        features_raw = self.settings.get("update.features")
        if features_raw is None:
            features_raw = ",".join(self.info.extras) or "all"
        self.features_suffix = self._parse_features(features_raw)

        # Retrieve update channel (defaults to stable)
        self.channel = str(self.settings.get("update.channel", "stable")).lower()

    # --- Subclass API ---

    def describe(self) -> str:
        """Where updates come from, for the update widget's status line."""
        raise NotImplementedError

    def fetch(self, force: bool = False) -> bool:
        raise NotImplementedError

    def get_versions(self, remote: bool = False) -> list[str]:
        raise NotImplementedError

    def install(self, version: str) -> bool:
        """Installs version. Returns True if the process must exit for the install to finish."""
        raise NotImplementedError

    # --- Shared ---

    def _parse_features(self, features_raw: str) -> str:
        if not features_raw:
            return ""
        clean_features = ",".join([f.strip() for f in features_raw.split(",") if f.strip()])
        return f"[{clean_features}]" if clean_features else ""

    def _is_version_allowed(self, tag: str) -> bool:
        """Filters versions based on the selected update channel."""
        try:
            v = parse_version(tag)
        except InvalidVersion:
            return False

        if self.channel == "stable":
            # Stable: No pre-releases (alpha, beta, rc) and no dev releases
            return not v.is_prerelease and not v.is_devrelease

        elif self.channel == "rc":
            # RC: Allow stable releases and specifically 'rc' pre-releases. Reject dev/alpha/beta.
            if v.is_devrelease:
                return False
            if v.is_prerelease:
                # v.pre is a tuple like ('rc', 1) or ('a', 0)
                return v.pre is not None and v.pre[0] == "rc"
            return True  # It's a stable release

        elif self.channel == "dev":
            # Dev: Unrestricted (stable, rc, alpha, beta, dev)
            return True

        # Fallback to stable if an unknown channel is set
        return not v.is_prerelease and not v.is_devrelease

    def _filter_and_sort(self, versions) -> list[str]:
        valid = [v for v in versions if self._is_version_allowed(v)]
        return sorted(valid, key=parse_version, reverse=True)

    def _fetch_due(self, force: bool) -> bool:
        """Cooldown shared by both sources: update.cooldown_seconds if set, else once per day."""
        from datetime import datetime

        last_fetch_ts = self.settings.get("update.last_fetch_time", 0)
        if force or not last_fetch_ts:
            return True

        custom_cooldown = self.settings.get("update.cooldown_seconds")
        if custom_cooldown is not None:
            # Option A: Fixed seconds-based cooldown
            return time() - last_fetch_ts >= int(custom_cooldown)

        # Option B: Date-change logic (Default)
        last_date = datetime.fromtimestamp(last_fetch_ts).date()
        if last_date == datetime.now().date():
            print(f"[Updater] Fetch skipped. Already checked today ({last_date}).")
            return False
        return True

    def _record_fetch(self):
        """After a successful fetch: stamp the cooldown and cache the newest version on this
        channel, which the CLI's "new version available" notice reads without going online."""
        self.settings.set("update.last_fetch_time", time(), scope="global")
        latest = self.get_latest_version()
        self.settings.set("update.latest_version", latest.lstrip("v") if latest else None, scope="global")

    def get_latest_version(self) -> str | None:
        versions = self.get_versions(remote=False)
        return versions[0] if versions else None

    @staticmethod
    def setup_command() -> list[str]:
        """`blink setup` in the freshly installed version: compiles its Numba kernels right after
        the install instead of on the next start. Through the interpreter (the install keeps its
        path), not the `blink` script, which doesn't have to be on PATH."""
        return [sys.executable, "-m", "blinkview", "setup"]

    @staticmethod
    def other_instances() -> list[RunningInstance]:
        """What has to be gone before an install can work: the detached Windows install only
        waits for this process. Always empty elsewhere, where a running program's files can be
        replaced."""
        if sys.platform != "win32":
            return []
        return find_other_instances()

    @classmethod
    def ensure_no_other_instances(cls):
        """Raises UpdateError naming whatever else is still running from this install."""
        others = cls.other_instances()
        if others:
            raise UpdateError(
                "BlinkView is still running elsewhere, and Windows can't replace its files until it "
                f"has closed: {', '.join(map(str, others))}. Close it and try again."
            )

    @classmethod
    def close_other_instances(cls, on_wait: Callable[[], None] | None = None):
        """The update widget's "Close and Continue" (see close_instances). Raises UpdateError
        naming the ones that are still there afterwards: a window that didn't finish closing in
        time or has a dialog open, a process of another user."""
        close_instances(cls.other_instances(), on_wait=on_wait)
        cls.ensure_no_other_instances()

    @staticmethod
    def wait_for_exit_command() -> str:
        """Blocks until this process is gone (a GUI with a session to flush takes longer to exit
        than any fixed delay), plus a moment for the blink.exe launcher above it, which is the
        file the installer can't overwrite while it runs."""
        import os

        return (
            "powershell -NoProfile -Command Wait-Process -Id "
            f"{os.getpid()} -Timeout {EXIT_WAIT_SECONDS} -ErrorAction SilentlyContinue; Start-Sleep 1"
        )

    @staticmethod
    def child_env() -> dict[str, str]:
        """The environment for the install and the `blink setup` behind it. Without the Numba
        cache dir this process exported for its own version (core/numba_setup.py): the new
        version's setup would compile into it, and its first start would look in its own dir and
        compile everything again. A dir the user chose is kept."""
        import os

        env = dict(os.environ)
        numba_setup = sys.modules.get("blinkview.core.numba_setup")  # not imported = nothing exported
        exported = getattr(numba_setup, "EXPORTED_CACHE_DIR", None)
        if exported is not None and env.get("NUMBA_CACHE_DIR") == exported:
            del env["NUMBA_CACHE_DIR"]
        return env

    def _run_install(self, cmd: list[str]) -> bool:
        """Windows can't replace the running blink.exe or loaded .pyds, so the install runs
        detached, once this process has exited (returns True = exit now).
        Either way `blink setup` follows a successful install, as its own process: this one
        still has the old version loaded."""
        import subprocess

        if sys.platform == "win32":
            cmd_str = subprocess.list2cmdline(cmd)
            setup_str = subprocess.list2cmdline(self.setup_command())
            # `&` after the wait: it reports failure when this process was already gone.
            # One retry: something else can hold the fresh blink.exe for a moment (a virus
            # scanner, the launcher still unwinding), and a second run finishes what the first
            # left half done.
            retry_str = f"powershell -NoProfile -Command Start-Sleep {INSTALL_RETRY_DELAY_SECONDS}"
            install_str = f"({cmd_str} || ({retry_str} & {cmd_str}))"
            detached_cmd = f'cmd /c "{self.wait_for_exit_command()} & {install_str} && {setup_str}"'
            subprocess.Popen(
                detached_cmd,
                shell=True,
                env=self.child_env(),
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
            )
            return True
        else:
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                raise UpdateError(f"Installation failed: {e.stderr or str(e)}")
            try:
                subprocess.Popen(self.setup_command(), start_new_session=True, env=self.child_env())
            except OSError as e:
                print(f"[Updater] Could not start `blink setup`: {e}")  # the next start compiles instead
            return False

    def upgrade(self, current_version: str) -> tuple[bool, str]:
        self.fetch()
        target = self.get_latest_version()

        if not target:
            raise UpdateError("No versions found.")

        if parse_version(target) <= parse_version(current_version):
            return False, target

        self.install(target)
        return True, target

    def clear_pending_status(self):
        """Removes the pending version flag from settings."""
        self.settings.set("update.pending_version", None, scope="global")

    def check_version_status(self, current_version_str: str) -> tuple[bool | None, str | None]:
        """
        Compares the current app version against the pending version.
        Returns (Success, VersionString) or (None, None) if no update was pending.
        """
        pending = self.settings.get("update.pending_version")
        if not pending:
            return None, None

        # Clear the flag immediately so we don't nag the user on every launch
        self.settings.set("update.pending_version", None, scope="global")

        try:
            v_current = parse_version(current_version_str)
            v_pending = parse_version(pending)

            # Success is defined as the current version being equal to or newer
            # than what we tried to install.
            return v_current >= v_pending, pending
        except Exception:
            # Fallback for invalid version strings
            return current_version_str == pending, pending


class GitUpdater(Updater):
    """Source checkout: versions are git tags in update.path, install = checkout + reinstall."""

    source = UpdateSource.GIT

    def __init__(self, settings: SettingsManager | None = None, info: InstallInfo | None = None):
        super().__init__(settings, info)

        # Pull configuration directly from the manager
        src_path = self.settings.get("update.path")
        if not src_path or not self.is_valid_repo(src_path):
            raise UpdateError("Update path not set. Run: blink config --global update.path /path/to/repo")

        self.repo_path = Path(src_path).resolve()

        is_editable_val = str(self.settings.get("update.editable", "")).lower()
        self.editable = is_editable_val in ["true", "1", "yes"]

    def describe(self) -> str:
        return f"git checkout at {self.repo_path}"

    @staticmethod
    def is_valid_repo(path: Path | str) -> bool:
        """
        Checks if a path is a valid BlinkView source tree.
        Can be called without instantiating the class.
        """
        p = Path(path)
        # Basic Git check
        if not (p / ".git").is_dir():
            return False

        # Identity check via pyproject.toml
        if not (p / "pyproject.toml").exists():
            return False

        # Source check
        main_py = p / "src" / "blinkview" / "__main__.py"
        return main_py.is_file()

    def fetch(self, force: bool = False) -> bool:
        """
        Runs git fetch if the cooldown has expired or if force is True.
        Returns True if a network fetch was performed, False otherwise.
        """
        import subprocess

        if not self._fetch_due(force):
            return False

        # Execute Network Call
        try:
            print("[Updater] Contacting remote for updates...")
            subprocess.run(
                ["git", "-C", str(self.repo_path), "fetch", "--tags"], check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Fetch failed: {e.stderr or e.stdout or str(e)}")

        self._record_fetch()
        return True

    def get_versions(self, remote: bool = False) -> list[str]:
        import subprocess

        cmd = ["git", "-C", str(self.repo_path), "tag", "-l"]
        if remote:
            cmd = ["git", "-C", str(self.repo_path), "ls-remote", "--tags", "origin"]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            lines = result.stdout.strip().splitlines()
            if remote:
                tags = [line.split("refs/tags/")[-1] for line in lines if "refs/tags/" in line]
            else:
                tags = lines
            # Apply channel filtering before sorting
            return self._filter_and_sort(tags)

        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Failed to list versions: {e.stderr or str(e)}")

    def install(self, version: str) -> bool:
        import subprocess

        self.ensure_no_other_instances()

        # Mark that we are attempting an upgrade to this specific version
        self.settings.set("update.pending_version", version, scope="global")

        try:
            subprocess.run(
                ["git", "-C", str(self.repo_path), "checkout", version], check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            # If git fails, we haven't actually started the install yet
            self.settings.set("update.pending_version", None, scope="global")
            raise UpdateError(f"Failed to checkout {version}: {e.stderr or str(e)}")

        install_target = f"{self.repo_path}{self.features_suffix}"
        cmd = ["uv", "tool", "install", install_target, "--python", sys.executable, "--force", "--refresh"]

        if self.editable:
            cmd.append("--editable")

        return self._run_install(cmd)


class PyPIUpdater(Updater):
    """Package install: versions come from the index's JSON API, install = pinned reinstall with
    the installer that owns this environment (uv tool, else pip)."""

    source = UpdateSource.PYPI

    def __init__(self, settings: SettingsManager | None = None, info: InstallInfo | None = None):
        super().__init__(settings, info)
        # JSON API base, e.g. https://test.pypi.org/pypi for a TestPyPI dry run.
        self.index_url = str(self.settings.get("update.index_url") or DEFAULT_INDEX_URL).rstrip("/")

    def describe(self) -> str:
        via = "uv tool" if self.info.installer == Installer.UV_TOOL else "pip"
        host = "PyPI" if self.index_url == DEFAULT_INDEX_URL else self.index_url
        return f"{host} via {via}"

    def fetch(self, force: bool = False) -> bool:
        if not self._fetch_due(force):
            return False

        import requests

        url = f"{self.index_url}/blinkview/json"
        try:
            print("[Updater] Contacting package index for updates...")
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as e:
            raise UpdateError(f"Fetch failed ({url}): {e}")

        versions = self.available_versions(data)
        self.settings.set("update.index_versions", versions, scope="global")
        self._record_fetch()
        return True

    @staticmethod
    def available_versions(data: dict) -> list[str]:
        """Versions from a PyPI JSON response that have at least one installable (non-yanked)
        file. Unparseable version strings are dropped."""
        out = []
        for ver, files in (data.get("releases") or {}).items():
            if not any(not f.get("yanked", False) for f in files):
                continue
            try:
                parse_version(ver)
            except InvalidVersion:
                continue
            out.append(ver)
        return out

    def get_versions(self, remote: bool = False) -> list[str]:
        if remote:
            self.fetch(force=True)
        return self._filter_and_sort(self.settings.get("update.index_versions") or [])

    def install_command(self, version: str) -> list[str]:
        requirement = f"blinkview{self.features_suffix}=={version.lstrip('v')}"

        if self.info.installer == Installer.UV_TOOL:
            cmd = ["uv", "tool", "install", requirement, "--python", sys.executable, "--force"]
            if self.index_url != DEFAULT_INDEX_URL:
                cmd += ["--index", self._simple_url()]
            return cmd

        cmd = [sys.executable, "-m", "pip", "install", requirement]
        if self.index_url != DEFAULT_INDEX_URL:
            cmd += ["--index-url", self._simple_url(), "--extra-index-url", PYPI_SIMPLE_URL]
        return cmd

    def _simple_url(self) -> str:
        """https://test.pypi.org/pypi -> https://test.pypi.org/simple (the installers' index API)."""
        base = self.index_url
        return (base[: -len("/pypi")] if base.endswith("/pypi") else base) + "/simple"

    def install(self, version: str) -> bool:
        self.ensure_no_other_instances()
        self.settings.set("update.pending_version", version, scope="global")
        try:
            return self._run_install(self.install_command(version))
        except UpdateError:
            self.settings.set("update.pending_version", None, scope="global")
            raise


def get_update_notice() -> str | None:
    """The CLI's "new version available" line, from the version the last fetch cached (see
    Updater._record_fetch) - never goes online, so every `blink` command stays fast."""
    from blinkview import __version__
    from blinkview.utils.global_settings import GlobalSettings

    latest = GlobalSettings().get("update.latest_version")
    if not latest:
        return None
    try:
        if parse_version(latest) <= parse_version(__version__):
            return None
    except InvalidVersion:
        return None
    return f"New version available: v{latest} (current: v{__version__}). Run 'blink update upgrade' to install it."
