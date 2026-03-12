from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str = "alocals3"
    api_prefix: str = "/s3"
    storage_root: Path = Path("./data")
    database_url: str = "sqlite:///./alocals3.db"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        app_name = os.getenv("ALOCALS3_APP_NAME", "alocals3")
        api_prefix = os.getenv("ALOCALS3_API_PREFIX", "/s3")
        storage_root = Path(os.getenv("ALOCALS3_STORAGE_ROOT", "./data"))
        database_url = os.getenv("ALOCALS3_DATABASE_URL", "sqlite:///./alocals3.db")
        log_level = os.getenv("ALOCALS3_LOG_LEVEL", "INFO")
        return cls(
            app_name=app_name,
            api_prefix=api_prefix,
            storage_root=storage_root,
            database_url=database_url,
            log_level=log_level,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
