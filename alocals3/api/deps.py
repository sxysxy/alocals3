from __future__ import annotations

from functools import lru_cache

from alocals3.core.config import get_settings
from alocals3.db import create_db_engine, create_session_factory, init_db
from alocals3.storage.local import LocalStorageBackend
from alocals3.storage.rust import RustLocalStorageBackend


@lru_cache(maxsize=1)
def get_storage() -> LocalStorageBackend | RustLocalStorageBackend:
    settings = get_settings()
    if settings.database_url.startswith("sqlite"):
        try:
            return RustLocalStorageBackend(settings.storage_root, settings.database_url)
        except RuntimeError:
            pass

    engine = create_db_engine(settings.database_url)
    init_db(engine)
    session_factory = create_session_factory(engine)
    return LocalStorageBackend(settings.storage_root, session_factory)
