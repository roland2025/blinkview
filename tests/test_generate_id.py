# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from unittest.mock import patch

from blinkview.utils.generate_id import generate_id


class TestGenerateId:
    def test_no_prefix_returns_bare_hex(self):
        with patch("blinkview.utils.generate_id.token_hex", return_value="deadbeef"):
            assert generate_id() == "deadbeef"

    def test_prefix_without_trailing_underscore_gets_one_added(self):
        with patch("blinkview.utils.generate_id.token_hex", return_value="deadbeef"):
            assert generate_id("src") == "src_deadbeef"

    def test_prefix_with_trailing_underscore_is_not_doubled(self):
        with patch("blinkview.utils.generate_id.token_hex", return_value="deadbeef"):
            assert generate_id("src_") == "src_deadbeef"

    def test_prev_none_accepts_the_first_candidate(self):
        with patch("blinkview.utils.generate_id.token_hex", return_value="deadbeef"):
            assert generate_id("src", prev=None) == "src_deadbeef"

    def test_retries_until_a_value_not_in_prev_is_found(self):
        with patch(
            "blinkview.utils.generate_id.token_hex",
            side_effect=["aaaaaaaa", "aaaaaaaa", "bbbbbbbb"],
        ):
            result = generate_id("src", prev=["src_aaaaaaaa"])
            assert result == "src_bbbbbbbb"

    def test_returns_immediately_when_candidate_not_in_prev(self):
        with patch("blinkview.utils.generate_id.token_hex", return_value="cafebabe") as mock_hex:
            result = generate_id(prev=["other_value"])
            assert result == "cafebabe"
            assert mock_hex.call_count == 1

    def test_real_call_produces_an_8_char_hex_string_by_default(self):
        result = generate_id()
        assert len(result) == 8
        int(result, 16)  # must be valid hex
