# BlinkView Roadmap

Here is a running list of ideas for future improvement, in no specific order and with no particular timeline.

---

## 🛠️ Core Infrastructure & Performance

Focusing on maximizing throughput, optimizing resource utilization, and stabilizing core APIs.

### Interactive Plot-to-Log Navigation

* Implement bidirectional cross-probing. Clicking a data point in the Telemetry Plotter automatically snaps the Text Log Viewer to the corresponding log entry.

### Automated Clock-Skew Calibration

* Develop an algorithmic calibration wizard to calculate and compensate for transport latency variations across asynchronous sources (e.g., ADB protocol lag vs. direct wire UART/RTT).

### Reorder layer smart delay period

* reorder layer should automaticaly increase the delay up to X seconds if zephyr or some other defferred logging situation is discovered

---

## 🌐 Advanced Extensions & Remote Operations

Expanding deployment environments and handling more complex multi-device hardware topologies.

### Headless Mode with Remote GUI Client (ZMQ)

* Decouple the backend ingestion pipeline from the UI. Allow the BlinkView core to run headlessly on a remote test bench or Raspberry Pi gateway, streaming data via ZeroMQ (ZMQ) to a local desktop GUI.

### RTEdbg Integration

* Add native support for Real-Time Execution debugging (RTEdbg) protocols to capture low-overhead kernel and task-switching traces.

### External Signal Ingestion (Saleae / Sigrok)

* Enable parsing and overlaying logic analyzer captures (`.pcap`, `.csv`) to correlate hardware-level line transitions (SPI, I2C, GPIO) directly with high-level software logs.

---

## 🧠 Analytics & Automation Ecosystem

Leveraging modern AI and developer abstractions to automate root-cause analysis.

### Local LLM Integration & Custom Backends

Integrate lightweight, local LLM backends (via Ollama / llama.cpp)

* analyze unified logs for anomalies, explain cryptic error cascading across components
* Auto-generate parser configurations from raw text samples.

### Declarative System Assertions (HIL Testing)

* Introduce an engine to define cross-device system assertions for continuous integration or Hardware-in-the-Loop setups (e.g., `expect(Android_Btn_Press).followed_by(CAN_ID_0x102)`).

### Extensible Plugin Architecture

* Expose a formal Python API for third-party developers to write custom data parsers, custom DBC decoders, or specialized telemetry rendering widgets.

---

## 🎨 UI/UX Enhancements

Improving workspace flexibility, developer daily workflow, and visual accessibility.

### High Refresh Rate ImGui Plotting

* Explore upgrading the telemetry visualization engine to use Dear ImGui (like ImPlot) for hardware-accelerated, ultra-high refresh rate plotting that keeps the UI smooth even under heavy data loads.

### Bidirectional Hex & Type Converter

* Add a built-in data manipulation UI widget to parse and convert raw hex dumps, registers, or binary payloads into readable configurations, and vice versa.
* Support instant translation of hex bytes into standard primitives (ASCII/UTF-8 strings, unsigned integers, floats) as well as machine timestamps (e.g., converting a hex string or hardware tick count into a human-readable calendar date-time, and back).
* **Contextual Selection:** Selecting raw text or hex patterns inside the log viewer automatically pops open an inspector sidebar showing all available data type conversions on the fly.

### Manual cross-device log synchronization

Allows aligning logs manually from multiple sources.

* **Define anchor points:** Select matching transient edges (e.g., GPIO state toggles) between different device views in the UI.
* **Calculate clock skew and offset:** Run a linear regression ($T = mx + c$) on the remote-provided timestamps (`remote_ts`) to find the clock frequency drift ($m$) and boot offset ($c$).
* **Dynamic viewport shift:** Apply the calculated scale and offset factors to the UI timeline coordinates without modifying the underlying database.
* **Persist synchronization metadata:** Save anchor pairs to configuration file.
