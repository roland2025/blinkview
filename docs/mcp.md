# Using BlinkView with an AI agent (MCP)

`blink mcp` lets an AI agent such as Claude Code work with a running BlinkView: read logs and
telemetry, send commands to a device, and, when you allow it, change the configuration.

Three things have to be in place:

1. The profile has an enabled pipeline of type `mcp_server`.
2. BlinkView is running with that profile.
3. The agent knows the `blink mcp` command.

## Quick start: a new project

In the folder you start the agent from (usually your firmware repository):

```text
blink init --agent
blink
```

`blink init --agent` does three things, and overwrites nothing that already exists:

- makes the folder a BlinkView project (`.blinkview/`),
- gives the project's profile an `mcp_server` pipeline that may change the configuration,
- registers `blink mcp` in the folder's `.mcp.json`, which Claude Code reads.

Then start the agent in the same folder and ask it to set up your board, for example: "I have
an STM32 on J-Link RTT printing `[ms] MODULE: text`, set it up".

If you don't know the format, say so: "it's on COM5 at 115200, I don't know
what it sends, work it out". The agent then adds a `raw_hex` pipeline to the source, which shows
the bytes as hexdump rows next to the normal output, works out the framing and message layout
from the dump, sets up the parser, and turns `raw_hex` off again. The dump stops by itself after
`max_bytes` (a million bytes by default).

Once the lines come through, the agent can also take values out of them, for example "plot the
battery voltage from the `bat:` lines". It adds extraction rules that turn `key=value` pairs,
a word after a given text, a word at a given position, or CSV-like fields into values of their
own. Open the telemetry table, a plot or a watch list in BlinkView to see them.

`--mcp` is a second spelling of `--agent`.

## A new profile in an existing project

```text
blink switch NAME --create --agent
```

This creates the profile with the same configuration. Without `--create` it fills an existing
profile that has no configuration yet. A profile that already has one is refused; add the
pipeline by hand instead (next section).

## A profile you already use

1. Open the settings in BlinkView and add a pipeline of type `mcp_server` under Pipelines.
2. Enable it and set what the agent may do (see the table below).
3. Register the command with the agent, once:

   ```text
   claude mcp add blinkview -- blink mcp
   ```

   Or add it to `.mcp.json` in the project folder yourself:

   ```json
   {
     "mcpServers": {
       "blinkview": { "command": "blink", "args": ["mcp"] }
     }
   }
   ```

## What the agent may do

The `mcp_server` pipeline's options are the permissions. The agent cannot change them itself.

| Option | Default | Allows |
| --- | --- | --- |
| `send_to_any` | on | Sending text and source commands (reset, halt, ...) to every source. |
| `send_to` | empty | Used when `send_to_any` is off: only these sources accept text and commands. |
| `allow_rotate` | on | Ending the current session and starting a new one. |
| `allow_read` | on | Reading log lines, the module list, telemetry values and the configuration. |
| `allow_config` | off | Changing sources, pipelines and storage settings. `blink init --agent` and `blink switch --agent` turn this on. |
| `allow_watches` | on | Creating, changing and opening watch lists. |
| `allow_post` | on | Logging its progress as lines of its own (see `post` below). |
| `port` | 0 | Local TCP port. 0 picks a free one; `blink mcp` finds it either way. |

Every configuration and watch list change is checked before it is applied and shown in BlinkView
with an Undo button.

When a change is turned off, the agent is told not to make it by editing the profile's files
instead, and to ask you. This is an instruction to the agent, not a lock on the files.

## Tools the agent gets

| Tool | Does |
| --- | --- |
| `status` | Lists running BlinkView instances, the current session and the sources. |
| `send`, `command` | Sends text to a source and returns the reply; runs a source's own command (e.g. RTT `reset`). |
| `read_memory`, `probe_status` | Reads target memory and the probe's state through a J-Link source's open session (see [J-Link](jlink.md#reading-the-target-from-an-ai-agent)). |
| `read_logs`, `summary` | Reads log lines with the log viewer's filters; counts lines per module and level. |
| `list_modules`, `telemetry_latest`, `telemetry_series` | Modules, their latest values, a signal's history. |
| `get_config`, `list_types`, `get_schema` | Reads the configuration and what can be configured. |
| `apply_config_patch` | Changes the configuration, or reverts an earlier change. |
| `create_watch`, `open_view` | Builds a watch list and opens its window. |
| `post` | Logs the agent's progress as lines and values of its own, to follow in the log, telemetry and watch windows. |
| `rotate_session` | Starts a new session. |

The agent's own lines are logged under the `mcp_server` pipeline's name (`mcp` in a profile made by `blink init --agent`), on modules it picks, such as `mcp dut2.flash` or `mcp dut2.flash.progress` for a value. Its `send`s, `command`s and J-Link reads are logged there too, under `tx.<source name>`, just before the device's reply. Add them to a watch list, or filter the log view on a device and `mcp`, to see what the agent is doing on each device. They are recorded with the session like any other line. When one agent works with several BlinkView instances, `post` can log the same step on each.

For a long stretch of log, `read_logs` and `send` take `to_file`: the lines go to a file in the
temp folder (`<temp>/blinkview/read_logs/`, deleted after a day) and the agent gets its path and
searches it, instead of reading up to a million lines. The file has the same lines as
`blink export`.

## Security

- The server listens on `127.0.0.1` only and requires a token that is new on every run.
- The token is kept in `~/.blinkview/run/`, never in the project folder.
- Everything the agent does is written to the log.
- There is no read-only switch for the whole server: turn off `send_to_any`, `allow_rotate`,
  `allow_config` and `allow_watches` to get one.

## When it does not work

- **The agent finds no instance.** BlinkView must be running, and the active profile must have
  an *enabled* `mcp_server` pipeline. `status` is the tool to check with.
- **Several BlinkView windows are open.** The agent picks one by the `pid` that `status` shows.
- **A configuration change is refused.** `allow_config` is off, or the change is outside
  sources, pipelines and storage settings, or it would leave the configuration invalid; the
  answer says which.
- **The agent does not see the `blinkview` tools.** Start it in the folder that holds
  `.mcp.json`, or register the command with `claude mcp add`.
