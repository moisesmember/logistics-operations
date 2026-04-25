import logging

from fastapi import APIRouter, Depends, status

from logistics_ops.api.dependencies import get_sync_use_case
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
