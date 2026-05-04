# BlinkView AI Agent Guidelines

This document provides context and rules for AI coding assistants working on the BlinkView project.

## Project Overview
BlinkView is a high-performance telemetry, log viewer, and visualization tool for embedded systems. It handles high-throughput data streams (UART, CAN, RTT, ADB, Sockets) and visualizes them in a multi-window PySide6 UI.

**Core Philosophy:**
- **Performance First:** The critical path (ingestion, parsing, reordering, filtering) must be lock-free where possible, minimize allocations, and use pre-allocated buffers.
- **Numba JIT Compilation:** Heavily utilized for data processing bottlenecks (string manipulation, searching, array operations).
- **Separation of Concerns:** Strict division between data ingestion/processing (the `core`/`ops` pipeline) and the UI (`ui`).

## Architecture & Directory Structure

- `src/blinkview/core/`: The foundational data structures, memory pools, registries, and configuration managers. This is the heart of the state management.
  - *Key concepts:* `NumpyBatchManager`, `CentralStorage`, `IDRegistry`, `PipelineManager`.
- `src/blinkview/ops/`: High-performance, Numba-accelerated operational logic. These functions typically operate on Numpy arrays and byte buffers.
  - *Rule:* Code in here must generally be compatible with `numba.njit`. Avoid Python objects (lists, dicts, strings) inside the JIT loops; use numpy arrays and raw bytes.
- `src/blinkview/io/`: Data ingestion sources (UART, RTT, ADB, Sockets). They read bytes from the hardware/OS and push them into the processing pipeline.
- `src/blinkview/parsers/`: Logic for decoding specific log formats or protocols into the structured internal representation.
- `src/blinkview/storage/`: Logic for saving raw or structured logs to disk.
- `src/blinkview/ui/`: The PySide6 graphical user interface.
  - *Rule:* The UI should never block on data processing. It reads from the central storage or subscribes to updates.
- `src/blinkview/utils/`: General helper functions.

## Coding Rules & Guidelines

### Performance & Memory Management
- **Zero-Allocation Critical Path:** When processing incoming data streams, avoid allocating new Python objects inside the main loop.
- **Use Memory Pools:** Utilize the pre-allocated buffers provided by `core` (e.g., `array_pool.py`, `numpy_buffer_pool.py`) for temporary data.
- **Numba Types:** When writing code for the `ops/` directory, ensure strict type signatures using `core.dtypes`.
- **String Handling:** In JIT-compiled code, strings are treated as raw byte arrays. Conversion to Python strings should only happen at the UI boundary.

### Numba Constraints (`ops/`)
- Functions annotated with `@njit` must be deterministic regarding types.
- Avoid using classes or custom objects inside Numba functions unless they are explicitly defined using `@jitclass` (which should be used sparingly).
- Rely on NumPy arrays (`np.ndarray`) for passing structured data.

### UI Development (`ui/`)
- **Qt Abstraction (`qtpy`):** While the underlying engine is PySide6, **ALL** Qt imports must be routed through the `qtpy` compatibility layer. 
  - *Strict Rule:* Never import directly from `PySide6`. 
  - *Correct:* `from qtpy.QtCore import Qt`, `from qtpy.QtWidgets import QWidget`.
- **Responsiveness:** Heavy filtering or data fetching must not freeze the main thread. Use background tasks or optimized Numba queries.
- **State Management:** UI components should save and restore their state using the mechanisms provided in `gui_context.py` and the component's `get_state()` / `restore()` methods.
- **Theming:** Adhere to the established styling, respecting dark mode configurations.

### Data Types (`core/dtypes.py`)
- Always use the predefined NumPy dtypes from `core.dtypes` (e.g., `ID_TYPE`, `LEVEL_TYPE`, `SEQ_TYPE`) when dealing with structured log data to ensure consistency across the pipeline and Numba compatibility.

### Dependency Management
- The project uses `uv` for dependency management.
- Do not introduce new heavy dependencies without explicit justification, especially if they impact performance or bundle size.

### Concurrency
- The ingestion pipeline runs in background threads.
- Communication between the background threads and the UI thread must be thread-safe, typically using signals/slots in Qt or thread-safe queues/pools in the core.

### Code Style
- Use type hints wherever possible outside of Numba-compiled code.
- Follow PEP 8 guidelines.
- Use descriptive variable names, especially when dealing with complex array indexing in the `ops/` directory.
