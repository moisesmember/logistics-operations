import logging

from logistics_ops.application.dto.ingestion_result import IngestionResult
from logistics_ops.domain.ports.dataset_source import DatasetSource
from logistics_ops.domain.ports.object_storage import ObjectStorage

logger = logging.getLogger(__name__)


class SyncDatasetToObjectStorage:
    """Synchronizes a dataset source into object storage without re-uploading existing files."""

    def __init__(
        self,
        source: DatasetSource,
        storage: ObjectStorage,
        bucket: str,
        prefix: str,
    ) -> None:
        self._source = source
        self._storage = storage
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def execute(self) -> IngestionResult:
        logger.info(
            "Starting dataset synchronization.",
            extra={"bucket": self._bucket, "prefix": self._prefix},
        )
        self._storage.ensure_bucket(self._bucket)

        dataset_assets = self._source.list_assets()
        logger.info("Discovered %s dataset files for synchronization.", len(dataset_assets))
        uploaded_files = 0
        skipped_files = 0

        for asset in dataset_assets:
            object_name = self._build_object_name(asset.relative_path)
            if self._storage.object_exists(self._bucket, object_name):
                logger.info("Skipping existing object '%s'.", object_name)
                skipped_files += 1
                continue

            logger.info("Uploading '%s' to bucket '%s'.", object_name, self._bucket)
            self._storage.upload_file(
                bucket=self._bucket,
                object_name=object_name,
                file_path=asset.local_path,
                content_type=asset.content_type,
            )
            uploaded_files += 1

        result = IngestionResult(
            bucket=self._bucket,
            prefix=self._prefix,
            total_files=len(dataset_assets),
            uploaded_files=uploaded_files,
            skipped_files=skipped_files,
        )
        logger.info(
            "Dataset synchronization finished: total=%s uploaded=%s skipped=%s.",
            result.total_files,
            result.uploaded_files,
            result.skipped_files,
        )
        return result

    def _build_object_name(self, relative_path: str) -> str:
        sanitized_path = relative_path.replace("\\", "/").strip("/")
        return f"{self._prefix}/{sanitized_path}"
