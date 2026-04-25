from fastapi import APIRouter

from logistics_ops.api.schemas.common import HealthResponse

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Verifica a saúde da API",
    description="Endpoint simples para validar se a API FastAPI está disponível.",
    response_description="API operacional.",
    operation_id="healthcheck",
)
def healthcheck() -> HealthResponse:
    return HealthResponse(status="ok")
