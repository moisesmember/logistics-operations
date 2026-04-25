from abc import ABC, abstractmethod
from pathlib import Path


class ObjectStorage(ABC):
    @abstractmethod
    def ensure_bucket(self, bucket: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def object_exists(self, bucket: str, object_name: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: Path,
        content_type: str | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        raise NotImplementedError
