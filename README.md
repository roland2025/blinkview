# BlinkView

**BlinkView** is a high-performance telemetry, log viewer, and visualization tool designed for embedded systems.

It provides real-time log streaming, structured parsing, key-value extraction, plotting, and multi-window monitoring — all optimized for high-throughput sources like UART, RTT, and sockets.

BlinkView helps you turn raw firmware logs into instant insight.

---

## Why BlinkView?

BlinkView is inspired by the first signal every embedded engineer knows: the blinking LED.

It focuses on:

- Fast insight
- Minimal friction
- Real-time visibility
- Embedded-friendly workflows

Launch your telemetry dashboard as easily as:

```bash
blink
```

---

## Features

- Real-time log viewing from multiple sources simultaneously.
- High-performance ingestion pipeline optimized for low overhead.
- **Supported Sources**:
  - UART/Serial ports
  - CAN-bus (with DBC file integration for signal decoding)
  - SEGGER RTT (Real Time Transfer)
  - Android Debug Bridge (ADB logcat) - *Note: Filtering needs improvement, feature is not yet finalized.*
- **Visualization & UI**:
  - Multi-window, tabbed log views.
  - **Advanced Log Viewer**:
    - Filter by device, module, and submodules.
    - Filter by log level (INFO, WARN, ERROR, etc.).
    - Toggle visibility of timestamp, device, level, and module columns.
    - Automatic auto-pause on high-velocity log bursts to prevent UI lockup.
    - Fast text search and highlighting.
  - Detachable windows for multi-monitor setups.
  - Live plotting of numeric telemetry data.
  - Watch/Command list for sending structured commands and monitoring specific variables.
  - Structured and configurable log parsing.
  - Automatic module/submodule hierarchy detection.
- **Core Engine**:
  - Timestamp alignment across different sources using high-precision clocks.
  - Raw data logging for lossless capture.
  - Centralized storage with a pub/sub architecture for UI updates.

---

## Performance

BlinkView is designed for high-throughput telemetry environments.

Features include:

- **Numba JIT Compilation**: Uses system-specific compiled backends via Numba for extreme performance. This powers core data processing (parsing, buffering, reordering), as well as fast filtering in text log views and high-speed telemetry plotting.
- Lock-efficient central storage
- Multi-threaded ingestion pipeline
- Timestamp reorder buffering
- Minimal UI overhead
- Efficient append-only log storage
- Capable of handling millions of log lines per session

---

## Architecture Overview

```mermaid
graph TD
    %% Source Nodes
    UART[UART Source]
    CAN[CAN Source]
    Socket[Socket Source]
    RTT[RTT Source]
    ADB[ADB Source]

    %% Pipeline Subgraphs
    subgraph UART_Pipe [UART Pipeline]
        UART_Raw[Raw File Writer]
        UART_P[Parser]
        UART_KV[KV Extractor]
    end

    subgraph CAN_Pipe [CAN Pipeline]
        CAN_Raw[Raw File Writer]
        CAN_P[Parser]
        CAN_KV[KV Extractor]
    end

    subgraph Sock_Pipe [Socket Pipeline]
        Sock_Raw[Raw File Writer]
        Sock_P[Parser]
        Sock_KV[KV Extractor]
    end

    subgraph RTT_Pipe [RTT Pipeline]
        RTT_Raw[Raw File Writer]
        RTT_P[Parser]
        RTT_KV[KV Extractor]
    end

    subgraph ADB_Pipe [ADB Pipeline]
        ADB_Raw[Raw File Writer]
        ADB_P[Parser]
        ADB_KV[KV Extractor]
    end

    %% Reorder Logic
    Reorder{Reorder Layer <br/> <i>Time-Delayed Buffer</i>}
    
    %% Central Hub
    Storage((Central Storage <br/> <i>Pub/Sub Provider</i>))

    %% Output Nodes
    LogUI[Log UI]
    KVPanel[KV Panel]
    Plotter[Plotter]
    UWriter[Unified File Writer]

    %% Flow within Pipelines
    UART --> UART_Raw
    UART --> UART_P
    UART_P --> UART_KV
    UART_P & UART_KV --> Reorder

    CAN --> CAN_Raw
    CAN --> CAN_P
    CAN_P --> CAN_KV
    CAN_P & CAN_KV --> Reorder

    Socket --> Sock_Raw
    Socket --> Sock_P
    Sock_P --> Sock_KV
    Sock_P & Sock_KV --> Reorder

    RTT --> RTT_Raw
    RTT --> RTT_P
    RTT_P --> RTT_KV
    RTT_P & RTT_KV --> Reorder

    ADB --> ADB_Raw
    ADB --> ADB_P
    ADB_P --> ADB_KV
    ADB_P & ADB_KV --> Reorder

    %% Chronological Alignment to Hub
    Reorder -- "Ordered Stream" --> Storage
    
    %% Pub/Sub Distribution
    Storage -.-> LogUI
    Storage -.-> KVPanel
    Storage -.-> Plotter
    Storage -.-> UWriter

    %% B&W Styling
    classDef bw fill:#fff,stroke:#000,stroke-width:2px,color:#000
    class UART,CAN,Socket,RTT,ADB,UART_P,CAN_P,Sock_P,RTT_P,ADB_P,UART_KV,CAN_KV,Sock_KV,RTT_KV,ADB_KV,UART_Raw,CAN_Raw,Sock_Raw,RTT_Raw,ADB_Raw,Reorder,Storage,LogUI,KVPanel,Plotter,UWriter bw
    
    %% Subgraph Styling
    style UART_Pipe fill:none,stroke:#000,stroke-dasharray: 5 5
    style CAN_Pipe fill:none,stroke:#000,stroke-dasharray: 5 5
    style Sock_Pipe fill:none,stroke:#000,stroke-dasharray: 5 5
    style RTT_Pipe fill:none,stroke:#000,stroke-dasharray: 5 5
    style ADB_Pipe fill:none,stroke:#000,stroke-dasharray: 5 5
```

Core logic is fully separated from the UI for performance and flexibility.

---

## Example Log Format

BlinkView supports flexible log formats such as:

```
[123.456] INFO main: System initialized
[123.789] WARN battery.current: 132 mA
[124.001] ERROR motor.driver: Overcurrent detected
```

Or custom formats via parser templates.

---

## Installation

### Requirements

- Python 3.10+
- PySide6 (for the UI)
- pyserial (for UART)
- numba (for high-performance data processing)

### UV package manager
UV is a modern Python tool for managing packages and tools.

For an optimal experience, BlinkView should be installed via uv. This ensures environment isolation and a simplified setup of all dependencies.

https://docs.astral.sh/uv/getting-started/installation/

#### UV on Windows

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Install from source

```bash
#Clone the repo
git clone https://github.com/roland2025/blinkview.git
cd blinkview

# install the tool to system
uv tool install ".[all]" --python 3.14
```

### Daemon mode (experimental)

```bash
# linux
UV_PROJECT_ENVIRONMENT=".venvd" uv sync --only-group daemon --python 3.14t

# windows powershell
$env:UV_PROJECT_ENVIRONMENT=".venvd"; uv sync --only-group daemon --python 3.14t
```

#### Upgrading

```bash
uv tool upgrade blinkview
```

---

## Usage


```bash
# go to your embedded project directory
cd your/mcu/project

# initialize the default profile
blink init

# start the GUI
blink
```

* if you initialized a project
  * profiles are saved in `./.blinkview/`
  * logs are saved in `./logs/`
* if you didnt init a project
  * profiles and logs are saved in `~/.blinkview/`

* recommended workflow:
  * initialize a profile for each project
  * start the GUI from the project directory
  * profiles can be added to git for team sharing
  * logs are in a separate directory and can be ignored in git

### Unified logs directory
By default, BlinkView saves logs in the current project directory under `./logs/`.

To centralize logs across projects, you can set a global log directory:

```bash
blink config --global log_dir /path/to/logs
```

---

## Supported Inputs

- **UART / Serial**: For classic embedded device logging.
- **CAN-bus**: Integrated with `cantools` for DBC-based signal extraction.
- **SEGGER RTT**: High-speed logging for ARM Cortex-M microcontrollers.
- **Android ADB**: Stream logs directly from Android devices via `logcat`. *Note: Filtering needs improvement, feature is not yet finalized.*
- **TCP / UDP Sockets**: For network-based telemetry streams.

---

## Design Goals

BlinkView is built with these priorities:

- Performance first
- Embedded-focused workflows
- Flexible parsing
- Multi-device support
- Non-blocking UI

---

## Use Cases

BlinkView is ideal for:

- Firmware development
- Embedded debugging
- Telemetry visualization
- Robotics development
- Battery system monitoring
- Real-time system analysis

---

## Status

BlinkView is currently in active development.

Core features are functional, and the architecture is designed for long-term extensibility.

---

## Roadmap

- File replay
- Text-to-speech alerts
- Advanced parsing and filtering options
- More plot types and customization
- Improved UI for configuration

---

## License

Mozilla Public License 2.0 (MPL-2.0)

---

## Name Origin

BlinkView is named after the first embedded program everyone writes:

```c
while (1)
{
    toggle_led();
}
```

The blink is the first signal that your system is alive.

BlinkView helps you see everything that follows.
