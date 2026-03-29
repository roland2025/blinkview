# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from can import Message
    from cantools import database

from ..core.configurable import configuration_property
from ..utils.paths import resolve_config_path
from .can_bus import CanDecoderFactory
from .transformer import TransformStep

# Assuming you have a generic factory for pipeline stages, similar to ParserFactory


def can_msg_to_str(msg_: "Message"):
    return f"{msg_.arbitration_id:04X} | {msg_.dlc} | {msg_.data.hex()}"


@CanDecoderFactory.register("cantools")
@configuration_property(
    "dbc_file",
    type="string",
    required=True,
    ui_type="file",
    ui_file_filter="DBC Files (*.dbc);;All Files (*)",
    description="Absolute or relative path to the .dbc database file.",
)
@configuration_property(
    "strict",
    type="boolean",
    default=False,
    required=True,
    ui_order=10,
    description="If true, raises an error for unknown CAN IDs. Overrides ignore_unknown.",
)
@configuration_property(
    "ignore_unknown",
    type="boolean",
    default=False,
    required=True,
    description="If true, silently ignores messages not defined in the DBC file by returning an empty dictionary.",
)
class CantoolsDecoder(TransformStep):
    __doc__ = """Decodes raw CAN frames into physical values using a DBC file.

    Injects `_msg_name` and `_can_id` into the resulting dictionary so downstream 
    assemblers can route the data to the correct UI modules.
    """

    dbc_file: str
    strict: bool
    ignore_unknown: bool

    def __init__(self):
        super().__init__()
        self.db = None
        self.process = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        if self.dbc_file:
            try:
                print(f"Loading DBC file: {self.dbc_file}")

                from cantools import database

                self.db = database.load_file(resolve_config_path(self.dbc_file))
            except Exception as e:
                print(f"Failed to load DBC file: {self.dbc_file}", e)
                self.db = None

        # --- Pre-bake Logic ---
        # Create a raw mapping of ID -> Message Object
        # This bypasses the overhead of the cantools database lookup methods.
        msg_map = {msg.frame_id: msg for msg in self.db.messages} if self.db else {}

        # Localize variables to the closure to avoid 'self' attribute lookups
        strict = self.strict
        ignore_unknown = self.ignore_unknown

        # Define the baked function
        def fast_process(can_msg: "Message") -> tuple[int, str, dict]:
            can_id = can_msg.arbitration_id

            # O(1) Dictionary lookup
            msg_def = msg_map.get(can_id)

            if msg_def is not None:
                try:
                    # Use the message object's internal decoder directly
                    return can_id, msg_def.name, msg_def.decode(can_msg.data)
                except Exception as e:
                    return (
                        can_id,
                        "ERROR",
                        {"error": f"{can_msg_to_str(can_msg)} | Decoding error: {str(e)}"},
                    )

            # --- Handle Unknown IDs ---
            if strict:
                raise ValueError(f"Unknown CAN ID: {can_id}")

            if ignore_unknown:
                return can_id, "IGNORED", {}

            return can_id, "UNMAPPED", {"unmapped": can_msg_to_str(can_msg)}

        # Bind the baked function
        self.process = fast_process

        return changed
