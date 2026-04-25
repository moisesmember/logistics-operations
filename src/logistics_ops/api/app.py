from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from logistics_ops.api.routers.health import router as health_router
from logistics_ops.api.routers.ingestion import router as ingestion_router
from logistics_ops.exceptions import DestinationAccessError, DestinationUnavailableError


def create_app() -> FastAPI:
    tags_metadata = [
        {
            "name": "health",
            "description": "Operações para monitoramento e verificação básica da API.",
        },
        {
            "name": "ingestion",
            "description": "Operações para sincronizar datasets do Kaggle no MinIO.",
        },
    ]

    app = FastAPI(
        title="Logistics Operations API",
        summary="API para orquestrar ingestão de datasets logísticos no MinIO.",
        version="0.1.0",
        description=(
            "API HTTP para orquestrar a ingestão do dataset de operações logísticas "
            "do Kaggle para o MinIO, com documentação automática via Swagger e ReDoc."
        ),
        contact={
            "name": "Logistics Operations",
            "url": "https://www.kaggle.com/datasets/yogape/logistics-operations-database",
        },
        license_info={
            "name": "MIT",
            "url": "https://opensource.org/licenses/MIT",
        },
        openapi_tags=tags_metadata,
        openapi_url="/api/v1/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
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
