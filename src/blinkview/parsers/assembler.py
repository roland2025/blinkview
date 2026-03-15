# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.BaseBindableConfigurable import BaseBindableConfigurable
from blinkview.core.factory import BaseFactory

from abc import ABC, abstractmethod
from typing import Any


class BaseAssembler(BaseBindableConfigurable):
    """
    The foundational contract for all Log Parsers.
    Takes a raw string and context, returning a structured LogRow.
    """

    # def __init__(self, **kwargs):
    #     """
    #     Accepts **kwargs to allow the Factory's Smart Dependency Injection
    #     to pass global context (like id_registry) safely.
    #     """
    #     pass
    #
    # def apply_config(self, config: dict):
    #     """
    #     Default configuration handler.
    #     Automatically maps JSON dictionary keys to class attributes
    #     if the attribute already exists on the instance.
    #     """
    #     pass

    # def bake(self):
    #     """
    #     Optional lifecycle hook.
    #     Override this to build high-speed closures or compile Regexes
    #     before the parser enters the 3 Mbps hot-loop.
    #     """
    #     pass
    #
    # @abstractmethod
    # def parse(self, timestamp: int, dev_id: Any, line: str) -> Any:
    #     """
    #     The Hot-Loop Method.
    #     MUST be implemented by the child class, or dynamically overridden
    #     during the bake() phase.
    #     """
    #     raise NotImplementedError("Parsers must implement parse() or override it in bake().")


class AssemblerFactory(BaseFactory[BaseAssembler]):
    pass
