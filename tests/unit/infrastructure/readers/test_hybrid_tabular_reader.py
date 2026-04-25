from pathlib import Path

from minio.error import S3Error

from logistics_ops.infrastructure.readers.hybrid_tabular_reader import HybridTabularReader
from logistics_ops.infrastructure.readers.local_tabular_reader import LocalTabularReader


class FailingPrimaryReader:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def list_dataset_objects(self) -> list[str]:
        raise self._exc

    def read_bytes(self, object_name: str) -> bytes:
        raise self._exc


def test_hybrid_reader_falls_back_to_local_when_minio_is_unavailable(
    workspace_tmp_dir: Path,
) -> None:
    (workspace_tmp_dir / "drivers.csv").write_text(
        "driver_id,name\nDRV001,Ana\n",
        encoding="utf-8",
    )
    local_reader = LocalTabularReader(
        dataset_root=workspace_tmp_dir,
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )
    hybrid_reader = HybridTabularReader(
        primary_reader=FailingPrimaryReader(OSError("MinIO offline")),
        fallback_reader=local_reader,
    )

    dataframe = hybrid_reader.read_csv_from_dataset("drivers.csv")

    assert dataframe.to_dict(orient="records") == [{"driver_id": "DRV001", "name": "Ana"}]


def test_hybrid_reader_falls_back_to_local_listing_when_object_is_missing(
    workspace_tmp_dir: Path,
) -> None:
    (workspace_tmp_dir / "drivers.csv").write_text(
        "driver_id,name\nDRV001,Ana\n",
        encoding="utf-8",
    )
    local_reader = LocalTabularReader(
        dataset_root=workspace_tmp_dir,
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )
    missing_object_error = S3Error(
        code="NoSuchKey",
        message="missing",
        resource="/drivers.csv",
        request_id="req-1",
        host_id="host-1",
        response=None,
    )
    hybrid_reader = HybridTabularReader(
        primary_reader=FailingPrimaryReader(missing_object_error),
        fallback_reader=local_reader,
    )

    object_names = hybrid_reader.list_dataset_objects()

    assert object_names == ["raw/kaggle/yogape/logistics-operations-database/drivers.csv"]
