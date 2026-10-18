# Flashing through the logging port

On an ESP32 (and most Arduino-style boards) the flasher and BlinkView use the same serial port.
While BlinkView has the port open, an upload fails with "port busy". **Port borrowing** lets the
flasher borrow the port from a running BlinkView: BlinkView closes it, the flasher writes the new
firmware, and BlinkView opens the port again as soon as the flasher is done. You keep your session,
your views and your filters, and with one extra setting you also get the new firmware's boot log.

## Turn it on for the serial source

In the serial source's settings:

| Setting | Default | Notes |
| --- | --- | --- |
| `allow_borrow` | off | Lend the port when a tool asks for it. |
| `borrow_name` | empty | The name the tool borrows the port by. Empty means the port's own name: `COM5`, or `ttyUSB0` for `/dev/ttyUSB0`. Set it for a `socket://` URL, or to give a board a stable name. A path (anything with `/` or `\`) is used as the borrow file itself. |
| `borrow_timeout_s` | `120` | Take the port back if a borrow hasn't been returned after this many seconds. `0` means never. |
| `reset_after_return` | off | Reset the board (DTR/RTS) right after the port comes back, so BlinkView reads its boot log from the first line. Pair it with esptool's `--after no_reset`, see below. |
| `serial_exclusive` | on | Open the port so no other program can open it at the same time. See [Linux and macOS](#linux-and-macos). |

The source's log shows `Lends the port on request: <file>` when it starts, and `Lent to pio
(pid 1234)` / `Returned after 8.4 s` around each upload. Its `state.lent` logger reads `1` while
the port is lent, so a lent port can be told apart from a broken one, where `state.open` alone
reads `0` for both.

## Borrow from the command line

```
blink borrow COM5 -- <command ...>
```

runs the command with the port borrowed and returns it afterwards, also when the command fails.
The exit code is the command's. For example:

```
blink borrow COM5 -- pio run -t upload
blink borrow ttyUSB0 -- idf.py -p /dev/ttyUSB0 flash
blink borrow COM5 -- arduino-cli upload -p COM5 --fqbn esp32:esp32:esp32
```

For build systems that have a "before" and an "after" step, borrow and return separately:

```
blink borrow COM5            # waits until BlinkView has closed the port
...flash...
blink return COM5
```

If the "after" step never runs (the upload failed and the build stopped), the borrow still ends
when the process that ran `blink borrow` exits: the shell, `make`, or PlatformIO's build process.
`--pid` names a different process.

If no BlinkView is running, or none lends this port, `blink borrow` waits 3 seconds
(`--timeout`), prints `no blink is lending COM5 (...), continuing`, and goes ahead: a flash never
fails because BlinkView is closed. `--strict` makes that an error instead.

`blink borrow --status` lists the current borrows and the folder they are in.

## PlatformIO

```
blink borrow --pio-script > blink_borrow.py
```

writes a small PlatformIO extra script next to your `platformio.ini`. Then:

```ini
[env:esp32dev]
extra_scripts = pre:blink_borrow.py
upload_port = COM5
; optional: don't reset after flashing, BlinkView does it once it's listening
upload_flags = --after=no_reset
```

and turn on `reset_after_return` in the BlinkView source.

`upload_port` is needed: the script runs before PlatformIO auto-detects the port, and it uses the
port as the borrow name. If `blink` isn't on the PATH of the VS Code terminal, set the `BLINK`
environment variable to the full path of the `blink` command.

Uploading from the PlatformIO toolbar in VS Code then works with BlinkView running.

## VS Code tasks, CMake and others

Anything that runs a command can wrap it:

```json
{
  "label": "Flash",
  "type": "shell",
  "command": "blink borrow COM5 -- idf.py -p COM5 flash"
}
```

## Without the blink command

The protocol is two files in `~/.blinkview/borrow/` (`$BLINKVIEW_BORROW_DIR` overrides the
folder), named after the borrow name:

1. The tool deletes `<name>.lent` if it is there, then writes `<name>.borrow`, optionally with
   JSON content: `{"pid": <process whose exit ends the borrow>, "ts": <unix time>, "by": "<tool>"}`.
2. BlinkView closes the port and writes `<name>.lent`. Wait for that file (BlinkView checks once a
   second). If it doesn't appear within a few seconds, no BlinkView is lending the port.
3. Flash, then delete `<name>.borrow`. BlinkView deletes `<name>.lent` and opens the port again.

Windows reserves the names `CON`, `PRN`, `AUX`, `NUL`, `COM0`-`COM9` and `LPT0`-`LPT9`, even
with an extension (`COM5.borrow` would be the serial port itself), so these names get an
underscore: the files for `COM5` are `COM5_.borrow` and `COM5_.lent`. This is done on every OS.
`COM10` and up are not reserved.

When a source starts, it deletes its own `.borrow` and `.lent` files. A borrow file at that point
is a leftover, or belongs to a tool that already has the port, in which case BlinkView can't open
the port anyway and keeps retrying until the tool lets go.

## Linux and macOS

On Windows only one program can have a COM port open. On Linux and macOS several can, and a
flasher and BlinkView reading the same port at the same time garble each other's data. With
`serial_exclusive` (on by default) BlinkView locks the port when it opens it (`TIOCEXCL` and
pyserial's exclusive lock): any other program that opens it gets "port busy" instead, until the
port is borrowed or BlinkView closes it.
