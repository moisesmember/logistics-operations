from unittest.mock import Mock, patch

import pandas as pd

from logistics_ops.notebooks import NotebookSession, build_notebook_session


def test_csv_file_names_returns_only_csv_names() -> None:
    reader = Mock()
    reader.list_dataset_objects.return_value = [
        "raw/kaggle/yogape/logistics-operations-database/drivers.csv",
        "raw/kaggle/yogape/logistics-operations-database/trips.csv",
        "raw/kaggle/yogape/logistics-operations-database/DATABASE_SCHEMA.txt",
    ]
    session = NotebookSession(reader=reader)

    file_names = session.csv_file_names()

    assert file_names == ["drivers.csv", "trips.csv"]


def test_load_csvs_returns_dictionary_of_dataframes() -> None:
    reader = Mock()
    reader.read_csv_from_dataset.side_effect = [
        pd.DataFrame([{"driver_id": "DRV001"}]),
        pd.DataFrame([{"trip_id": "TRIP001"}]),
    ]
    session = NotebookSession(reader=reader)

    dataframes = session.load_csvs(["drivers.csv", "trips.csv"])

    assert list(dataframes.keys()) == ["drivers.csv", "trips.csv"]
    assert dataframes["drivers.csv"].to_dict(orient="records") == [{"driver_id": "DRV001"}]
    assert dataframes["trips.csv"].to_dict(orient="records") == [{"trip_id": "TRIP001"}]


def test_summarize_csvs_returns_shape_summary() -> None:
    reader = Mock()
    reader.read_csv_from_dataset.side_effect = [
        pd.DataFrame([{"driver_id": "DRV001"}, {"driver_id": "DRV002"}]),
        pd.DataFrame([{"trip_id": "TRIP001", "distance": 100}]),
    ]
    session = NotebookSession(reader=reader)

    summary = session.summarize_csvs(["drivers.csv", "trips.csv"])

    assert summary.to_dict(orient="records") == [
        {"file_name": "drivers.csv", "rows": 2, "columns": 1},
        {"file_name": "trips.csv", "rows": 1, "columns": 2},
    ]


@patch("logistics_ops.notebooks.build_tabular_reader")
def test_build_notebook_session_applies_pandas_options(mock_build_tabular_reader: Mock) -> None:
    mock_build_tabular_reader.return_value = Mock()

    session = build_notebook_session(display_max_columns=None, display_width=140)

    assert isinstance(session, NotebookSession)
    assert pd.get_option("display.width") == 140
