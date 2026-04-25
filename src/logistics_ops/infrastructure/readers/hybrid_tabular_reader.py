import logging
from io import BytesIO, StringIO

import pandas as pd
from minio.error import S3Error

from logistics_ops.exceptions import DestinationAccessError, DestinationUnavailableError
from logistics_ops.infrastructure.readers.local_tabular_reader import LocalTabularReader
from logistics_ops.infrastructure.readers.minio_tabular_reader import MinioTabularReader

logger = logging.getLogger(__name__)


class HybridTabularReader:
    """Notebook-friendly reader that prefers MinIO and falls back to local Kaggle cache."""

    def __init__(
        self,
        primary_reader: MinioTabularReader,
        fallback_reader: LocalTabularReader,
    ) -> None:
        self._primary_reader = primary_reader
        self._fallback_reader = fallback_reader

    def list_dataset_objects(self) -> list[str]:
        try:
            logger.info("Trying to list dataset objects from MinIO first.")
            return self._primary_reader.list_dataset_objects()
        except self._fallback_exceptions() as exc:
            logger.warning(
                "Falling back to local dataset listing because MinIO is unavailable: %s",
                exc,
            )
            return self._fallback_reader.list_dataset_objects()

    def read_bytes(self, object_name: str) -> bytes:
        try:
            logger.info("Trying to read '%s' from MinIO first.", object_name)
            return self._primary_reader.read_bytes(object_name)
        except self._fallback_exceptions() as exc:
            logger.warning(
                "Falling back to local file for '%s' because MinIO is unavailable: %s",
                object_name,
                exc,
            )
            return self._fallback_reader.read_bytes(object_name)

    def read_bytes_from_dataset(self, file_name: str) -> bytes:
        return self.read_bytes(self._fallback_reader._dataset_object_name(file_name))

    def read_text(self, object_name: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(object_name).decode(encoding)

    def read_text_from_dataset(self, file_name: str, encoding: str = "utf-8") -> str:
        return self.read_text(self._fallback_reader._dataset_object_name(file_name), encoding)

    def read_csv(self, object_name: str, **pandas_kwargs) -> pd.DataFrame:
        return pd.read_csv(BytesIO(self.read_bytes(object_name)), **pandas_kwargs)

    def read_csv_from_dataset(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.read_csv(self._fallback_reader._dataset_object_name(file_name), **pandas_kwargs)

    def read_json(self, object_name: str, **pandas_kwargs) -> pd.DataFrame:
        return pd.read_json(StringIO(self.read_text(object_name)), **pandas_kwargs)

    def read_json_from_dataset(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.read_json(self._fallback_reader._dataset_object_name(file_name), **pandas_kwargs)

    @staticmethod
    def _fallback_exceptions() -> tuple[type[BaseException], ...]:
        return (
            S3Error,
            OSError,
            ConnectionError,
            DestinationUnavailableError,
            DestinationAccessError,
        )
