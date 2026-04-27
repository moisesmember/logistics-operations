from dataclasses import dataclass
import warnings

import pandas as pd

from logistics_ops.bootstrap import build_tabular_reader
from logistics_ops.infrastructure.readers.hybrid_tabular_reader import HybridTabularReader


@dataclass(slots=True)
class NotebookSession:
    reader: HybridTabularReader

    def list_dataset_objects(self) -> list[str]:
        return self.reader.list_dataset_objects()

    def csv_file_names(self) -> list[str]:
        dataset_objects = self.list_dataset_objects()
        return [object_name.split("/")[-1] for object_name in dataset_objects if object_name.endswith(".csv")]

    def load_csv(self, file_name: str, **pandas_kwargs) -> pd.DataFrame:
        return self.reader.read_csv_from_dataset(file_name, **pandas_kwargs)

    def load_csvs(
        self,
        file_names: list[str] | None = None,
        **pandas_kwargs,
    ) -> dict[str, pd.DataFrame]:
        resolved_file_names = file_names or self.csv_file_names()
        return {
            file_name: self.load_csv(file_name, **pandas_kwargs)
            for file_name in resolved_file_names
        }

    def summarize_csvs(
        self,
        file_names: list[str] | None = None,
        **pandas_kwargs,
    ) -> pd.DataFrame:
        dataframes = self.load_csvs(file_names=file_names, **pandas_kwargs)
        return pd.DataFrame(
            [
                {
                    "file_name": file_name,
                    "rows": dataframe.shape[0],
                    "columns": dataframe.shape[1],
                }
                for file_name, dataframe in dataframes.items()
            ]
        ).sort_values("file_name", ignore_index=True)


def build_notebook_session(
    *,
    ignore_warnings: bool = True,
    display_max_columns: int | None = None,
    display_width: int = 120,
) -> NotebookSession:
    if ignore_warnings:
        warnings.filterwarnings("ignore")

    pd.set_option("display.max_columns", display_max_columns)
    pd.set_option("display.width", display_width)

    return NotebookSession(reader=build_tabular_reader())
