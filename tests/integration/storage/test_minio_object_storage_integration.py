import pytest

from logistics_ops.infrastructure.readers.minio_tabular_reader import MinioTabularReader


@pytest.mark.integration
def test_upload_and_read_object_bytes(
    integration_storage,
    integration_bucket: str,
    sample_csv_file,
) -> None:
    object_name = "raw/kaggle/yogape/logistics-operations-database/drivers.csv"

    integration_storage.upload_file(
        bucket=integration_bucket,
        object_name=object_name,
        file_path=sample_csv_file,
        content_type="text/csv",
    )

    payload = integration_storage.get_object_bytes(integration_bucket, object_name)

    assert payload == b"driver_id,name\nDRV001,Ana\n"


@pytest.mark.integration
def test_list_objects_and_reader_roundtrip(
    integration_storage,
    integration_bucket: str,
    sample_csv_file,
) -> None:
    object_name = "raw/kaggle/yogape/logistics-operations-database/drivers.csv"
    integration_storage.upload_file(
        bucket=integration_bucket,
        object_name=object_name,
        file_path=sample_csv_file,
        content_type="text/csv",
    )
    reader = MinioTabularReader(
        storage=integration_storage,
        bucket=integration_bucket,
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    object_names = reader.list_dataset_objects()
    dataframe = reader.read_csv_from_dataset("drivers.csv")

    assert object_names == [object_name]
    assert dataframe.to_dict(orient="records") == [
        {"driver_id": "DRV001", "name": "Ana"}
    ]
