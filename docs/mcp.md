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
| `port` | 0 | Local TCP port. 0 picks a free one; `blink mcp` finds it either way. |

Every configuration and watch list change is checked before it is applied and shown in BlinkView
with an Undo button.

## Tools the agent gets

| Tool | Does |
| --- | --- |
| `status` | Lists running BlinkView instances, the current session and the sources. |
| `send`, `command` | Sends text to a source and returns the reply; runs a source's own command (e.g. RTT `reset`). |
| `read_logs`, `summary` | Reads log lines with the log viewer's filters; counts lines per module and level. |
| `list_modules`, `telemetry_latest`, `telemetry_series` | Modules, their latest values, a signal's history. |
| `get_config`, `list_types`, `get_schema` | Reads the configuration and what can be configured. |
| `apply_config_patch` | Changes the configuration, or reverts an earlier change. |
| `create_watch`, `open_view` | Builds a watch list and opens its window. |
| `rotate_session` | Starts a new session. |

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
