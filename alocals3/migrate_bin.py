from __future__ import annotations

import os
import subprocess
import sys
from importlib import resources


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    executable = "alocals3-migrate2pg.exe" if os.name == "nt" else "alocals3-migrate2pg"
    ref = resources.files("alocals3").joinpath("bin", executable)
    if not ref.is_file():
        raise SystemExit(f"bundled migration executable not found: {ref}")

    with resources.as_file(ref) as path:
        command = [str(path), *args]
        if os.name == "nt":
            return subprocess.call(command)
        os.execv(str(path), command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
