# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


def bindable(cls):
    """
    Class decorator that injects system binding capabilities,
    replacing the need for a BaseBindableConfigurable inheritance hierarchy.
    """
    # 1. Intercept the original __init__ to set up initial state
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        # Initialize the bindable attributes
        self.shared = None
        self.local = None
        self.logger = None

        # Execute the original __init__ (which might be the user's init,
        # or the one already wrapped by @configurable)
        original_init(self, *args, **kwargs)

    # Replace the class's init with our wrapped one
    cls.__init__ = new_init

    # 2. Define the method we want to inject
    def bind_system(self, shared, local):
        self.shared = shared
        self.local = local
        if hasattr(local, "get_logger"):
            self.logger = local.get_logger()

    # 3. Bind the injected method to the target class
    cls.bind_system = bind_system

    return cls
