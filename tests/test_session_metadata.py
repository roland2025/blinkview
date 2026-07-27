# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import subprocess
from unittest.mock import patch

from blinkview.utils.session_metadata import create_session_metadata, get_git_revision_hash


class TestGetGitRevisionHash:
    def test_returns_stripped_ascii_output_of_git_rev_parse(self):
        with patch("subprocess.check_output", return_value=b"8a2f3c1\n"):
            assert get_git_revision_hash() == "8a2f3c1"

    def test_returns_unknown_when_git_is_unavailable(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError()):
            assert get_git_revision_hash() == "unknown"

    def test_returns_unknown_when_not_in_a_git_repo(self):
        with patch(
            "subprocess.check_output",
            side_effect=subprocess.CalledProcessError(128, ["git"]),
        ):
            assert get_git_revision_hash() == "unknown"


class TestCreateSessionMetadata:
    def test_writes_metadata_json_with_expected_fields(self, tmp_path):
        with patch("blinkview.utils.session_metadata.get_git_revision_hash", return_value="deadbee"):
            create_session_metadata(tmp_path, "my_session")

        meta_file = tmp_path / "metadata.json"
        assert meta_file.is_file()

        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        assert meta["session_name"] == "my_session"
        assert meta["git_hash"] == "deadbee"
        assert meta["start_time_utc"].endswith("Z")
        assert "platform" in meta
        assert "python_version" in meta

    def test_extra_meta_is_merged_in_and_can_override_defaults(self, tmp_path):
        with patch("blinkview.utils.session_metadata.get_git_revision_hash", return_value="deadbee"):
            create_session_metadata(tmp_path, "my_session", extra_meta={"session_name": "overridden", "custom": 1})

        meta = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
        assert meta["session_name"] == "overridden"
        assert meta["custom"] == 1

    def test_no_extra_meta_leaves_defaults_untouched(self, tmp_path):
        with patch("blinkview.utils.session_metadata.get_git_revision_hash", return_value="deadbee"):
            create_session_metadata(tmp_path, "plain_session", extra_meta=None)

        meta = json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))
        assert meta["session_name"] == "plain_session"
