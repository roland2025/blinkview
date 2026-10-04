# BlinkView Developer Guide

This document covers the internal development environment, testing procedures, and performance constraints for the BlinkView project.

## Development Setup

BlinkView uses `uv` for lightning-fast dependency management and environment isolation.

### 1. Initialize Environment
```bash
# Sync all dependencies including development tools
uv sync --all-groups
```

### 2. Environment Variables
BlinkView is optimized for low-memory footprints. To prevent heavy math libraries (like OpenBLAS) from pre-allocating massive thread-pools on startup, ensure your `.env` file contains the following:

```ini
OPENBLAS_NUM_THREADS=1
MKL_NUM_THREADS=1
OMP_NUM_THREADS=1
```

### 3. Installing `blink` from your checkout

```bash
git clone https://github.com/roland2025/blinkview.git
cd blinkview
uv tool install ".[all]"          # add --editable to run straight from the working tree
```

A source install updates from git tags in the checkout rather than from PyPI. The first GUI launch asks
for the checkout path (`update.path`), and the Numba cache lives in `<checkout>/.numba_cache/<version>`.
To make a source install follow PyPI instead, run `blink config --global update.source pypi`.

## Releasing

```bash
uv run scripts/release.py patch     # or minor / major / release / an explicit x.y.z
```

This bumps `__version__`, commits, tags `vX.Y.Z` and pushes. The tag starts
`.github/workflows/publish.yml`, which builds the sdist and wheel and installs the wheel as a uv tool on
Python 3.11-3.14. It then runs `scripts/import_smoke.py` (every module must import) and publishes to PyPI
through Trusted Publishing. `.devN` and `rcN` tags are published too. PyPI hides them from a plain
`uv tool install blinkview`, so only the `dev` and `rc` update channels see them.

For a dry run, start the workflow by hand from the Actions tab with `index: testpypi`. To point an
install's updater at TestPyPI, run `blink config --global update.index_url https://test.pypi.org/pypi`.

## Testing

We use the standard library `unittest` framework for both logic and performance verification.

### Running All Tests
To run the full suite with discovery:
```bash
uv run pytest -v
```

### Running Specific Suites
If you are working on core logic or memory:
```bash
# Memory & Registry Integrity

uv run pytest tests/test_registry_memory.py  -m memory -v -s -k test_initial_import_baseline
uv run pytest tests/test_registry_memory.py -v

```

## Memory & Performance Standards

BlinkView aims for a lean idle footprint. Current benchmarks on Python 3.14 (Windows) should target:

| State | Target Private Bytes | Target USS |
| :--- | :--- | :--- |
| **Idle (Core Only)** | ~50 MB | ~40 MB |
| **Idle (with UI)** | ~200 MB | ~160 MB |

### Memory Guardrails
When adding new dependencies or global objects:
1. **Avoid Module-Level Allocations:** Do not initialize large arrays or buffers at the top level of a file. Use lazy-loading or internal factory methods.
2. **Import Cost:** Use `blinkview.utils.audit_imports` to verify the memory cost of new imports.
3. **Verify Threads:** If Private Bytes spike to >500MB, check if a new dependency is triggering multi-threaded BLAS initialization.

## Architecture Guidelines

- **UI/Core Separation:** Core logic (Registry, Storage, Parsers) must remain completely independent of `PySide6`. 
- **Thread Safety:** Use the `NumpyArrayPool` for high-frequency data buffers to reduce GC pressure.
- **Locking:** Prefer lock-free structures or fine-grained locks; avoid holding locks during I/O or UI updates.

## Profiling Tools

We have included several utilities to help monitor the system:

- `blinkview.utils.audit_imports`: Profiles the memory cost of each import step.
- `blinkview.utils.profile_memory`: A wrapper function to measure the USS and Private Bytes delta of a specific logic block.

## Experimental Features

### Daemon Mode
```bash
# Linux
UV_PROJECT_ENVIRONMENT=".venvd" uv sync --only-group daemon --python 3.14t

# Windows PowerShell
$env:UV_PROJECT_ENVIRONMENT=".venvd"; uv sync --only-group daemon --python 3.14t
```

---

### Format code

```bash
# Memory & Registry Integrity
uv run ruff format .
```