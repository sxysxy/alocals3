from fastapi import FastAPI

from alocals3.api.routes.buckets import router as buckets_router
from alocals3.api.routes.health import router as health_router
from alocals3.api.routes.objects import router as objects_router
from alocals3.core.config import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)

    app.include_router(health_router)
    app.include_router(buckets_router)
    app.include_router(objects_router)

    return app
