from __future__ import annotations

import logging
import time

from fastapi import FastAPI, Request

from alocals3.api.routes.buckets import router as buckets_router
from alocals3.api.routes.health import router as health_router
from alocals3.api.routes.objects import router as objects_router
from alocals3.core.config import get_settings
from alocals3.core.logging import health_counter, log_level_by_status, setup_logging


def create_app() -> FastAPI:
    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    setup_logging(level=log_level)
    logger = logging.getLogger("alocals3.api")
    app = FastAPI(title=settings.app_name)

    @app.middleware("http")
    async def request_logging_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        path = request.url.path
        should_log = path == "/healthz" or path.startswith("/s3")
        if not should_log:
            return await call_next(request)

        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = (time.perf_counter() - started) * 1000
            logger.error(
                "request_failed method=%s path=%s duration_ms=%.2f",
                request.method,
                path,
                duration_ms,
                exc_info=True,
            )
            raise

        duration_ms = (time.perf_counter() - started) * 1000
        status_code = response.status_code
        level = log_level_by_status(status_code)

        if path == "/healthz":
            # Health probes can be high-frequency; sample successful logs.
            hit = next(health_counter)
            if status_code >= 400 or hit % 30 == 0:
                logger.log(
                    level,
                    "health method=%s status=%s duration_ms=%.2f sampled=%s",
                    request.method,
                    status_code,
                    duration_ms,
                    status_code >= 400 or hit % 30 == 0,
                )
            return response

        bucket, key = _extract_bucket_key(path)
        logger.log(
            level,
            "s3 method=%s status=%s duration_ms=%.2f bucket=%s key=%s path=%s",
            request.method,
            status_code,
            duration_ms,
            bucket,
            key,
            path,
        )
        return response

    app.include_router(health_router)
    app.include_router(buckets_router)
    app.include_router(objects_router)

    return app


def _extract_bucket_key(path: str) -> tuple[str, str]:
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        return "-", "-"
    if parts[0] != "s3":
        return "-", "-"

    bucket = parts[1]
    if len(parts) == 2:
        return bucket, "-"
    if len(parts) >= 3 and parts[2] == "objects":
        return bucket, "-"

    key = "/".join(parts[2:])
    return bucket, key
