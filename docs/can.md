# CAN bus

BlinkView reads a CAN bus with a source of type `can`, through
[python-can](https://python-can.readthedocs.io/). A `cantools` pipeline then decodes the frames
with a DBC file.

## Install the adapter's driver separately

BlinkView does not include the drivers for CAN adapters. Install the one for your adapter from
its maker first:

- **PEAK (PCAN-USB and others):** the PEAK driver package from
  [peak-system.com](https://www.peak-system.com/), which includes the PCAN-Basic library.
  Interface `pcan`, channel e.g. `PCAN_USBBUS1`.
- **Kvaser:** *Kvaser Drivers for Windows* (which includes CANlib) from
  [kvaser.com/downloads](https://www.kvaser.com/downloads/). Interface `kvaser`, channel `0` for
  the first channel.

On Linux, both adapters also work through the kernel's SocketCAN drivers: bring the interface up
(`ip link set can0 up type can bitrate 500000`) and use interface `socketcan`, channel `can0`.

Then install BlinkView with the CAN backend: the `all`, `hardware` or `can` extra, e.g.
`uv tool install "blinkview[all]"`.

To check the driver, open the maker's own tool (PCAN-View, Kvaser CanKing) and see the bus
traffic there. Close it before starting BlinkView, since an adapter channel can usually be open
in only one program at a time.

## Source settings

| Setting | Default | Notes |
| --- | --- | --- |
| `interface` | `virtual` | `pcan`, `kvaser`, `socketcan`, `ixxat`, `slcan`, or `virtual` for testing without hardware. |
| `channel` | `vcan0` | Depends on the interface, see above. |
| `bitrate` | `250000` | Must match the bus. |

When the adapter can't be opened, BlinkView logs the error and tries again once a second.
