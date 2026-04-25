from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from logistics_ops.api.routers.health import router as health_router
from logistics_ops.api.routers.ingestion import router as ingestion_router
from logistics_ops.exceptions import DestinationAccessError, DestinationUnavailableError


def create_app() -> FastAPI:
    app = FastAPI(
        title="Logistics Operations API",
        version="0.1.0",
        description="HTTP API for orchestrating Kaggle to MinIO dataset ingestion.",
    )
    app.include_router(health_router)
    app.include_router(ingestion_router)

    @app.exception_handler(DestinationUnavailableError)
    async def handle_destination_unavailable(
        request: Request,
        exc: DestinationUnavailableError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": str(exc), "path": str(request.url.path)},
        )

    @app.exception_handler(DestinationAccessError)
    async def handle_destination_access_error(
        request: Request,
        exc: DestinationAccessError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=502,
            content={"detail": str(exc), "path": str(request.url.path)},
        )

    return app


app = create_app()
