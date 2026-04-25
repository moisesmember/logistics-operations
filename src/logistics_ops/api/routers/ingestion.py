import logging

from fastapi import APIRouter, Depends, status

from logistics_ops.api.dependencies import get_sync_use_case
from logistics_ops.api.schemas.common import ErrorResponse
from logistics_ops.api.schemas.ingestion import IngestionResponse
from logistics_ops.application.use_cases.sync_dataset_to_object_storage import (
    SyncDatasetToObjectStorage,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingestions", tags=["ingestion"])


@router.post(
    "/logistics-dataset/sync",
    response_model=IngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Sincroniza o dataset logístico no MinIO",
    description=(
        "Dispara a ingestão do dataset `yogape/logistics-operations-database` "
        "do Kaggle para o MinIO de forma idempotente."
    ),
    response_description="Resumo da sincronização executada.",
    operation_id="sync_logistics_dataset",
    responses={
        200: {
            "description": "Dataset sincronizado com sucesso.",
        },
        502: {
            "model": ErrorResponse,
            "description": "O destino respondeu, mas não pôde ser acessado corretamente.",
        },
        503: {
            "model": ErrorResponse,
            "description": "O MinIO não está disponível para receber a ingestão.",
        },
    },
)
def sync_logistics_dataset(
    use_case: SyncDatasetToObjectStorage = Depends(get_sync_use_case),
) -> IngestionResponse:
    logger.info("Received HTTP request to synchronize logistics dataset.")
    result = use_case.execute()
    logger.info(
        "HTTP synchronization finished: total=%s uploaded=%s skipped=%s.",
        result.total_files,
        result.uploaded_files,
        result.skipped_files,
    )
    return IngestionResponse(
        bucket=result.bucket,
        prefix=result.prefix,
        total_files=result.total_files,
        uploaded_files=result.uploaded_files,
        skipped_files=result.skipped_files,
    )
