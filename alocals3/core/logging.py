from __future__ import annotations

import itertools
import logging
from logging import Logger


def setup_logging(level: int = logging.INFO) -> Logger:
    # Keep setup minimal and deterministic. Avoid extra handler fan-out.
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    # Reduce duplicate/noisy access logs; app middleware provides structured request logs.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    return logging.getLogger("alocals3")


def log_level_by_status(status_code: int) -> int:
    if status_code >= 500:
        return logging.ERROR
    if status_code >= 400:
        return logging.WARNING
    return logging.INFO


health_counter = itertools.count(1)
