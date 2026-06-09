# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


def main(args=None):
    if args is None:
        from blinkview.ui.run import main as main_fn

        main_fn()
    else:
        from blinkview.ui.run import run as main_fn

        main_fn(args)


if __name__ == "__main__":
    main()
