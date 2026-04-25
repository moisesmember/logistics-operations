from io import BytesIO, StringIO

import pandas as pd

from logistics_ops.infrastructure.storage.minio_object_storage import MinioObjectStorage


class MinioTabularReader:
    """Reusable reader for notebooks and scripts that need dataset files from MinIO."""

    def __init__(
        self,
        storage: MinioObjectStorage,
        bucket: str,
        dataset_prefix: str,
    ) -> None:
        self._storage = storage
        self._bucket = bucket
        self._dataset_prefix = dataset_prefix.strip("/")

    def list_dataset_objects(self) -> list[str]:
        return self._storage.list_objects(self._bucket, self._dataset_prefix)

    def read_bytes(self, object_name: str) -> bytes:
        return self._storage.get_object_bytes(self._bucket, object_name)

    def read_bytes_from_dataset(self, file_name: str) -> bytes:
        return self.read_bytes(self._dataset_object_name(file_name))

    def read_text(self, object_name: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(object_name).decode(encoding)

    def read_text_from_dataset(self, file_name: str, encoding: str = "utf-8") -> str:
        return self.read_text(self._dataset_object_name(file_name), encoding=encoding)

    def read_csv(self, object_name: str, **pandas_kwargs) -> pd.DataFrame:
        content = self.read_bytes(object_name)
        return pd.read_csv(BytesIO(content), **pandas_kwargs)

    def read_csv_from_dataset(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.read_csv(self._dataset_object_name(file_name), **pandas_kwargs)

    def read_json(self, object_name: str, **pandas_kwargs) -> pd.DataFrame:
        content = self.read_text(object_name)
        return pd.read_json(StringIO(content), **pandas_kwargs)

    def read_json_from_dataset(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.read_json(self._dataset_object_name(file_name), **pandas_kwargs)

    def _dataset_object_name(self, file_name: str) -> str:
        return f"{self._dataset_prefix}/{file_name.strip('/')}"
