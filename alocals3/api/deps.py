from __future__ import annotations

from functools import lru_cache

from alocals3.core.config import get_settings
from alocals3.db import create_db_engine, create_session_factory, init_db
from alocals3.storage.local import LocalStorageBackend


@lru_cache(maxsize=1)
def get_storage() -> LocalStorageBackend:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    session_factory = create_session_factory(engine)
    return LocalStorageBackend(settings.storage_root, session_factory)
