# Release Build Scripts

These scripts build the Rust server binary and the Python wheel.

## Python Wheel ABI

The wheel is configured for Python 3.12+ with PyO3 limited ABI:

- `pyproject.toml`: `requires-python = ">=3.12"`
- `Cargo.toml`: `pyo3` feature `abi3-py312`

This produces a CPython 3.12+ ABI3 wheel, not a `cp312-cp312` wheel. That keeps the extension on Python's stable ABI while matching the project's Python 3.12 baseline. To build a CPython-3.12-only wheel instead, remove `abi3-py312` from the `pyo3` dependency features and build with a Python 3.12 interpreter.

The runtime Python dependency list is intentionally empty in `pyproject.toml`; networking is implemented in Rust.

The server build steps use the `server,server-binary` features and set `PYO3_NO_PYTHON=1` because the server binary does not embed Python. This prevents a local Python older than 3.12 from breaking a server-only build.

## Linux

```bash
scripts/build-linux-release.sh
```

Defaults:

- server target: `x86_64-unknown-linux-musl`
- server output: `dist/alocals3-server-linux-x86_64-unknown-linux-musl`
- wheel builder: `ghcr.io/pyo3/maturin:v1.7`

Useful overrides:

```bash
TARGET=aarch64-unknown-linux-musl scripts/build-linux-release.sh
USE_DOCKER=0 PYTHON=python3.12 scripts/build-linux-release.sh
```

## Windows

Run from PowerShell on Windows 10 or newer:

```powershell
.\scripts\build-windows-release.ps1
```

Defaults:

- server output: `dist\alocals3-server-windows.exe`
- Python resolver: `py -3.12`

Useful overrides:

```powershell
$env:PYTHON = "C:\Path\To\Python312\python.exe"
$env:TARGET = "x86_64-pc-windows-msvc"
.\scripts\build-windows-release.ps1
```

## macOS

Run on macOS with Xcode command line tools:

```bash
scripts/build-macos-release.sh
```

Defaults:

- target: `aarch64-apple-darwin`
- deployment target: `MACOSX_DEPLOYMENT_TARGET=11.0`
- server output: `dist/alocals3-server-macos-aarch64-apple-darwin`

Useful overrides:

```bash
PYTHON=python3.12 scripts/build-macos-release.sh
```
