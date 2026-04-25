import logging
from io import StringIO
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class LocalTabularReader:
    """Reader for cached dataset files stored on the local filesystem."""

    def __init__(self, dataset_root: Path, dataset_prefix: str) -> None:
        self._dataset_root = dataset_root
        self._dataset_prefix = dataset_prefix.strip("/")

    def list_dataset_objects(self) -> list[str]:
        logger.info("Listing local dataset files from '%s'.", self._dataset_root)
        return [
            self._dataset_object_name(file_path.relative_to(self._dataset_root).as_posix())
            for file_path in sorted(
                path for path in self._dataset_root.rglob("*") if path.is_file()
            )
        ]

    def read_bytes(self, object_name: str) -> bytes:
        file_path = self._resolve_path(object_name)
        logger.info("Reading local file '%s'.", file_path)
        return file_path.read_bytes()

    def read_bytes_from_dataset(self, file_name: str) -> bytes:
        return self.read_bytes(self._dataset_object_name(file_name))

    def read_text(self, object_name: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(object_name).decode(encoding)

    def read_text_from_dataset(self, file_name: str, encoding: str = "utf-8") -> str:
        return self.read_text(self._dataset_object_name(file_name), encoding=encoding)

    def read_csv(self, object_name: str, **pandas_kwargs) -> pd.DataFrame:
        file_path = self._resolve_path(object_name)
        logger.info("Reading local CSV '%s'.", file_path)
        return pd.read_csv(file_path, **pandas_kwargs)

    def read_csv_from_dataset(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.read_csv(self._dataset_object_name(file_name), **pandas_kwargs)

    def read_json(self, object_name: str, **pandas_kwargs) -> pd.DataFrame:
        content = self.read_text(object_name)
        return pd.read_json(StringIO(content), **pandas_kwargs)

    def read_json_from_dataset(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.read_json(self._dataset_object_name(file_name), **pandas_kwargs)

    def _resolve_path(self, object_name: str) -> Path:
        normalized_name = object_name.replace("\\", "/").strip("/")
        prefix = f"{self._dataset_prefix}/"
        relative_name = (
            normalized_name[len(prefix) :]
            if normalized_name.startswith(prefix)
            else normalized_name
        )
        return self._dataset_root / relative_name

    def _dataset_object_name(self, file_name: str) -> str:
        return f"{self._dataset_prefix}/{file_name.strip('/')}"
