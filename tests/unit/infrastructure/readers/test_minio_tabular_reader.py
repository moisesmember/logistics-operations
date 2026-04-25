import pandas as pd

from logistics_ops.infrastructure.readers.minio_tabular_reader import MinioTabularReader


class StubStorage:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self._payloads = payloads

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        return self._payloads[object_name]

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        return sorted(
            [object_name for object_name in self._payloads if object_name.startswith(prefix)]
        )


def test_read_csv_from_dataset_reads_dataframe() -> None:
    storage = StubStorage(
        {
            "raw/kaggle/yogape/logistics-operations-database/drivers.csv": (
                b"driver_id,name\nDRV001,Ana\nDRV002,Caio\n"
            )
        }
    )
    reader = MinioTabularReader(
        storage=storage,
        bucket="logistics-lake",
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    dataframe = reader.read_csv_from_dataset("drivers.csv")

    assert list(dataframe.columns) == ["driver_id", "name"]
    assert dataframe.to_dict(orient="records") == [
        {"driver_id": "DRV001", "name": "Ana"},
        {"driver_id": "DRV002", "name": "Caio"},
    ]


def test_read_text_from_dataset_returns_text() -> None:
    storage = StubStorage(
        {
            "raw/kaggle/yogape/logistics-operations-database/DATABASE_SCHEMA.txt": (
                b"drivers -> trips"
            )
        }
    )
    reader = MinioTabularReader(
        storage=storage,
        bucket="logistics-lake",
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    text = reader.read_text_from_dataset("DATABASE_SCHEMA.txt")

    assert text == "drivers -> trips"


def test_read_json_from_dataset_reads_dataframe() -> None:
    storage = StubStorage(
        {
            "raw/kaggle/yogape/logistics-operations-database/metrics.json": (
                b'[{"driver_id":"DRV001","score":98}]'
            )
        }
    )
    reader = MinioTabularReader(
        storage=storage,
        bucket="logistics-lake",
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    dataframe = reader.read_json_from_dataset("metrics.json")

    assert isinstance(dataframe, pd.DataFrame)
    assert dataframe.to_dict(orient="records") == [{"driver_id": "DRV001", "score": 98}]


def test_list_dataset_objects_uses_dataset_prefix() -> None:
    storage = StubStorage(
        {
            "raw/kaggle/yogape/logistics-operations-database/drivers.csv": b"x",
            "raw/kaggle/yogape/logistics-operations-database/trips.csv": b"y",
            "other-prefix/ignored.csv": b"z",
        }
    )
    reader = MinioTabularReader(
        storage=storage,
        bucket="logistics-lake",
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    object_names = reader.list_dataset_objects()

    assert object_names == [
        "raw/kaggle/yogape/logistics-operations-database/drivers.csv",
        "raw/kaggle/yogape/logistics-operations-database/trips.csv",
    ]
