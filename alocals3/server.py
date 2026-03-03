from __future__ import annotations

import argparse
import os

import uvicorn

from alocals3.api.deps import get_storage
from alocals3.app import create_app
from alocals3.core.config import get_settings

app = create_app()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run alocals3 server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    parser.add_argument("--reload", action="store_true", help="Enable auto reload")
    parser.add_argument("--database-url", default=None, help="SQLAlchemy database URL")
    parser.add_argument("--storage-root", default=None, help="Object storage root directory")
    parser.add_argument("--app-name", default=None, help="Application name")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.database_url:
        os.environ["ALOCALS3_DATABASE_URL"] = args.database_url
    if args.storage_root:
        os.environ["ALOCALS3_STORAGE_ROOT"] = args.storage_root
    if args.app_name:
        os.environ["ALOCALS3_APP_NAME"] = args.app_name

    get_settings.cache_clear()
    get_storage.cache_clear()

    cli_app = create_app()
    uvicorn.run(cli_app, host=args.host, port=args.port, reload=args.reload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
