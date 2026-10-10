# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The configuration a profile starts with. Shared by the Registry (a profile with no config
file yet) and the CLI that writes one (`blink switch NAME --create --agent`), so nothing here may
pull in numba or Qt."""

AGENT_PIPELINE_ID = "pipe_mcp"


def default_profile_config() -> dict:
    """A new dict each call: the ConfigManager keeps and edits the one it is given."""
    return {
        "version": "0.2",
        "sources": {},
        "pipelines": {},
        "plugins": {},
        "reorder": {"enabled": True, "type": "default"},
        "central": {"enabled": True, "type": "default"},
    }


def agent_profile_config() -> dict:
    """The default configuration plus an `mcp_server` pipeline (parsers/mcp_server.py) that may
    change the configuration, so an agent can set the profile up from nothing. `allow_config` is
    off by default and the agent cannot turn it on itself."""
    config = default_profile_config()
    config["pipelines"][AGENT_PIPELINE_ID] = {
        "enabled": True,
        "type": "mcp_server",
        "name": "mcp",
        "allow_config": True,
    }
    return config
