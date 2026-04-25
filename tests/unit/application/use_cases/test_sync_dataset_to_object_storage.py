from pathlib import Path

from logistics_ops.application.use_cases.sync_dataset_to_object_storage import (
    SyncDatasetToObjectStorage,
)
from logistics_ops.domain.entities.dataset_asset import DatasetAsset
from logistics_ops.domain.ports.dataset_source import DatasetSource
from logistics_ops.domain.ports.object_storage import ObjectStorage


class StubDatasetSource(DatasetSource):
    def __init__(self, assets: list[DatasetAsset]) -> None:
        self._assets = assets

    def list_assets(self) -> list[DatasetAsset]:
        return self._assets


class SpyObjectStorage(ObjectStorage):
    def __init__(self, existing_objects: set[str] | None = None) -> None:
        self.existing_objects = existing_objects or set()
        self.created_buckets: list[str] = []
        self.upload_calls: list[tuple[str, str, Path, str | None]] = []

    def ensure_bucket(self, bucket: str) -> None:
        self.created_buckets.append(bucket)

    def object_exists(self, bucket: str, object_name: str) -> bool:
        return object_name in self.existing_objects

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: Path,
        content_type: str | None = None,
    ) -> None:
        self.upload_calls.append((bucket, object_name, file_path, content_type))

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        raise NotImplementedError

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        raise NotImplementedError


def test_execute_uploads_only_missing_files(sample_assets: list[DatasetAsset]) -> None:
    storage = SpyObjectStorage(
        existing_objects={"raw/kaggle/yogape/logistics-operations-database/drivers.csv"}
    )
    use_case = SyncDatasetToObjectStorage(
        source=StubDatasetSource(sample_assets),
        storage=storage,
        bucket="logistics-lake",
        prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    result = use_case.execute()

    assert storage.created_buckets == ["logistics-lake"]
    assert result.total_files == 2
    assert result.uploaded_files == 1
    assert result.skipped_files == 1
    assert storage.upload_calls == [
        (
            "logistics-lake",
            "raw/kaggle/yogape/logistics-operations-database/nested/DATABASE_SCHEMA.txt",
            sample_assets[1].local_path,
            "text/plain",
        )
    ]


def test_build_object_name_normalizes_windows_separators(workspace_tmp_dir: Path) -> None:
    asset = DatasetAsset(
        relative_path=r"nested\drivers.csv",
        local_path=workspace_tmp_dir / "drivers.csv",
        content_type="text/csv",
    )
    storage = SpyObjectStorage()
    use_case = SyncDatasetToObjectStorage(
        source=StubDatasetSource([asset]),
        storage=storage,
        bucket="bucket",
        prefix="/raw/dataset/",
    )

    result = use_case.execute()

    assert result.prefix == "raw/dataset"
    assert storage.upload_calls == [
        (
            "bucket",
            "raw/dataset/nested/drivers.csv",
            asset.local_path,
            "text/csv",
        )
    ]
