from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import select

from alocals3.core.config import get_settings
from alocals3.db import ObjectModel, create_db_engine, create_session_factory, init_db


def _iter_blob_files(objects_root: Path) -> list[Path]:
    if not objects_root.exists():
        return []
    return [p for p in objects_root.rglob("*") if p.is_file()]


def run(apply: bool = False) -> int:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    session_factory = create_session_factory(engine)

    objects_root = settings.storage_root.resolve() / "objects"
    blob_files = _iter_blob_files(objects_root)

    with session_factory() as session:
        referenced = set(session.scalars(select(ObjectModel.file_path)).all())

    orphan_files: list[Path] = []
    for path in blob_files:
        rel = path.relative_to(objects_root).as_posix()
        if rel not in referenced:
            orphan_files.append(path)

    reclaimed_bytes = sum(path.stat().st_size for path in orphan_files)
    print(f"objects_root={objects_root}")
    print(f"referenced_blobs={len(referenced)}")
    print(f"total_blob_files={len(blob_files)}")
    print(f"orphan_blob_files={len(orphan_files)}")
    print(f"reclaimable_bytes={reclaimed_bytes}")

    if not apply:
        print("dry-run mode: no files removed")
        return 0

    for path in orphan_files:
        path.unlink(missing_ok=True)

    # Best-effort empty-dir cleanup.
    for dir_path in sorted((p for p in objects_root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        try:
            if not any(dir_path.iterdir()):
                dir_path.rmdir()
        except OSError:
            pass

    print(f"removed_orphan_files={len(orphan_files)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Garbage collect orphan blob files")
    parser.add_argument("--apply", action="store_true", help="Actually delete orphan files")
    args = parser.parse_args(argv)
    return run(apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
