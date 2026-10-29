#!/usr/bin/env python3
"""Check 64-bit macOS binaries and wheels for dyld's string-pool alignment."""

import argparse
from pathlib import Path
import subprocess
import tempfile
import zipfile


def check_binary(path: Path, label: str) -> None:
    result = subprocess.run(
        ["otool", "-arch", "all", "-l", str(path)],
        check=True, capture_output=True, text=True,
    )
    offsets = [
        int(line.split()[1])
        for line in result.stdout.splitlines()
        if line.strip().startswith("stroff ")
    ]
    if not offsets:
        raise ValueError(f"{label}: no LC_SYMTAB string pool found")
    for offset in offsets:
        if offset % 8:
            raise ValueError(f"{label}: mis-aligned LINKEDIT string pool: {offset:#x}")
    print(f"OK {label}: stroff={', '.join(hex(offset) for offset in offsets)}")


def check_artifact(path: Path) -> None:
    if path.suffix != ".whl":
        check_binary(path, str(path))
        return
    with zipfile.ZipFile(path) as wheel, tempfile.TemporaryDirectory() as directory:
        members = [
            name for name in wheel.namelist()
            if name.endswith((".so", ".dylib"))
            or (name.startswith("alocals3/bin/") and not name.endswith("/"))
        ]
        if not any(name.endswith(".so") for name in members):
            raise ValueError(f"{path}: native extension missing")
        for name in members:
            binary = Path(directory) / "binary"
            binary.write_bytes(wheel.read(name))
            check_binary(binary, f"{path}:{name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        for path in args.artifacts:
            check_artifact(path)
    except (OSError, ValueError, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"LINKEDIT validation failed: {exc}\n")


if __name__ == "__main__":
    main()
