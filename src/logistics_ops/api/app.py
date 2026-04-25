from fastapi import FastAPI

from logistics_ops.api.routers.health import router as health_router
from logistics_ops.api.routers.ingestion import router as ingestion_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Logistics Operations API",
        version="0.1.0",
        description="HTTP API for orchestrating Kaggle to MinIO dataset ingestion.",
    )
    app.include_router(health_router)
    app.include_router(ingestion_router)
    return app


app = create_app()
