from pathlib import Path

from logistics_ops.infrastructure.readers.local_tabular_reader import LocalTabularReader


def test_local_reader_reads_csv_from_dataset(workspace_tmp_dir: Path) -> None:
    (workspace_tmp_dir / "drivers.csv").write_text(
        "driver_id,name\nDRV001,Ana\n",
        encoding="utf-8",
    )
    reader = LocalTabularReader(
        dataset_root=workspace_tmp_dir,
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    dataframe = reader.read_csv_from_dataset("drivers.csv")

    assert dataframe.to_dict(orient="records") == [{"driver_id": "DRV001", "name": "Ana"}]


def test_local_reader_lists_prefixed_dataset_objects(workspace_tmp_dir: Path) -> None:
    (workspace_tmp_dir / "drivers.csv").write_text("driver_id,name\nDRV001,Ana\n", encoding="utf-8")
    nested_dir = workspace_tmp_dir / "nested"
    nested_dir.mkdir()
    (nested_dir / "schema.txt").write_text("schema", encoding="utf-8")
    reader = LocalTabularReader(
        dataset_root=workspace_tmp_dir,
        dataset_prefix="raw/kaggle/yogape/logistics-operations-database",
    )

    object_names = reader.list_dataset_objects()

    assert object_names == [
        "raw/kaggle/yogape/logistics-operations-database/drivers.csv",
        "raw/kaggle/yogape/logistics-operations-database/nested/schema.txt",
    ]
