# SEGGER J-Link and RTT

BlinkView reads SEGGER RTT through a J-Link probe with a source of type `jlink_rtt`. This page
covers what has to be installed, the source's settings, reading the target from an AI agent,
running several probes, and the problems seen so far.

## Install the J-Link software separately

BlinkView talks to the probe through SEGGER's J-Link library (via
[pylink](https://github.com/square/pylink)), which is **not** part of BlinkView. Install it
from SEGGER first:

1. Download the **J-Link Software and Documentation Pack** from
   [segger.com/downloads/jlink](https://www.segger.com/downloads/jlink/) and install it in its
   default location. This also installs the USB driver for the probe.
2. Install BlinkView with the J-Link backend: the `all`, `hardware` or `jlink` extra, e.g.
   `uv tool install "blinkview[all]"`.

To check the installation, open **J-Link Commander** (`JLink.exe`) from the SEGGER pack and
connect to the probe. If that works, BlinkView can use the probe too.

## Source settings

| Setting | Default | Notes |
| --- | --- | --- |
| `target_device` | `NRF52840_XXAA` | Exact name from SEGGER's device list, e.g. `STM32F407VG`. A family name such as `NRF52` does not connect. |
| `serial_number` | empty | The probe's serial number. Empty means the first J-Link found, so set it whenever more than one probe is attached. |
| `interface` | `swd` | `swd` or `jtag`. |
| `speed` | `4000` | Interface speed in kHz. |
| `channel` | `0` | RTT up-channel to read. 0 is the standard terminal channel. |
| `target_rtt_buffer_size` | `8192` | Size of the RTT up-buffer in the firmware (`BUFFER_SIZE_UP`). When it connects, BlinkView uses it to drop the stale data still in the buffer, without discarding live data. |
| `delay` | `100` | Longest time (ms) incoming bytes are held before they're passed on. |
| `stall_warn_s` | `30` | Log a warning when no data has arrived for this many seconds. `0` turns it off. |
| `stall_reconnect_s` | `60` | Reconnect the probe when no data has arrived for this many seconds. `0` turns it off. |

The serial number list in the settings editor shows the probes attached right now.

When the probe drops off or the connection is lost, BlinkView retries once a second until the
probe answers again. You don't need to restart it.

RTT can also stall without any error: the probe still reports open and connected, but every read
returns nothing. When no data has arrived for `stall_warn_s`, BlinkView logs "No data for N s"
on the source's `link` logger, with the RTT status the J-Link DLL reports. After
`stall_reconnect_s` it closes the probe and connects again, the same as the `rtt_restart`
command. While the source stays silent it retries at doubling intervals, up to every 10 minutes.
After 3 attempts it also says to check the USB hubs: when every board goes quiet at once,
power-cycling them is what has fixed it. When data comes back, it logs "Data resumed after N s".
The time while the probe is not connected at all doesn't count. Set both to `0` for firmware
that is silent for long stretches on purpose.

Other source types have the same two settings, off by default. They only warn: reconnecting
works only for RTT so far.

An RTT source offers these commands, which an AI agent can also send over
[MCP](mcp.md): `reset` (reset the MCU), `halt`, `restart` (resume after a halt) and
`rtt_restart` (close the probe and connect again, which restarts RTT without resetting the
target).

## Reading the target from an AI agent

An agent connected over [MCP](mcp.md) can look inside the target through the probe session the
RTT source already has open. It doesn't need J-Link Commander or a second connection to the
probe, and RTT keeps streaming while it reads.

- **`read_memory`** reads up to 4 KB per call: variables, peripheral registers, the Cortex-M
  fault registers (CFSR at `0xE000ED28`, HFSR at `0xE000ED2C`, ...). It reads 8, 16 or 32 bits
  at a time (`width`). On Cortex-M a read doesn't halt the core. The address is a number or a
  string such as `"0x20001000"`. To read a variable, the agent looks up its address in the
  firmware's ELF file itself, e.g. with `arm-none-eabi-nm`.
- **`probe_status`** shows what the session sees: target voltage, whether the target is
  connected and halted, the core, the probe's serial number and speed, and the RTT buffers. Use
  it to tell a firmware problem from a wiring or probe problem.

Every call is logged as a `tx.<source name>` line, like the agent's commands, so the session
recording shows what it read and when. A call is refused while the source isn't connected, and
gives up after 2 s if the probe is busy (see [The probe stops answering](#the-probe-stops-answering)).
The same reads are available to scripts through `blinkview.client` (`read_memory()`,
`probe_status()`).

Known limit: the J-Link library may not describe the RTT down buffers (seen with an nRF52840).
`probe_status` then lists them as `{"empty": true}` with a `down_buffers_note`. Their names and
sizes are in the RTT control block in target memory, which `read_memory` can read.

## Several probes

There are two ways to use several probes. Either way, give every source its own
`serial_number`.

**In one BlinkView:** add a `jlink_rtt` source for each probe, so all the boards share one
timeline. For now each probe's source and pipeline are set up by hand, since there are no
device or pipeline templates yet.

**One BlinkView per board:** for identical boards, keep one profile and give each run its own
serial number with
[profile parameters](../README.md#several-identical-devices-profile-parameters):

```bash
blink switch rtt --add-param rtt_serial /sources/src_1eb62594/serial_number
blink switch rtt --save-params left  --param rtt_serial=51024923
blink switch rtt --save-params right --param rtt_serial=51099999

blink -p rtt --params left
blink -p rtt --params right
```

### Too many probes on one USB hub

Five J-Links sharing a single 6-port hub made one of them unreliable. The errors came and went
for minutes, in any order:

- `Cannot connect to J-Link` (each attempt takes about 10 s)
- `Communication timed out: Requested 4 bytes, received 0 bytes !`
- `No emulator with serial number ... found.` (the probe briefly drops off USB)
- `Target system has no power.`
- the probe stops answering in the middle of a session (see the next section)

Moving the probe to another hub fixed it. If these errors flap like this, spread the probes over
several hubs or USB ports, or use a hub with its own power supply, before suspecting the probe,
the cable or BlinkView.

## Troubleshooting

### The probe stops answering

The J-Link library can hang inside a call when the probe stops responding. BlinkView can't
interrupt that call, so while it lasts:

- no RTT data arrives and nothing is logged,
- commands are refused with `Command dropped: the J-Link is busy.`,
- when you close BlinkView, it prints where the reader is stuck and ends with
  `Exiting without unloading drivers: still blocked in a call: JLinkRTTReader(...)`.

That last message is expected. Unloading the J-Link driver while a call is stuck in it would
leave the process hanging, so BlinkView saves the session and ends the process without unloading
it. Unplug and replug the probe, and check the USB setup (see above).

### `J-Link is already open.`

Another process has the probe open through pylink: usually another BlinkView with the same
serial number, or an earlier BlinkView process that is still running. Check Task Manager (or
`ps`) for a leftover `blink` or `python` process. pylink keeps a lock file per probe,
`.pylink-usb-<serial>.lck`, in the temp folder, and ignores it once the process that wrote it
has exited.

### `J-Link does not know the target device '...'`

`target_device` isn't a name from SEGGER's device list. Use the full part name, e.g.
`NRF52840_XXAA`, not `NRF52`.
