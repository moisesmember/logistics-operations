import shutil
import uuid
from pathlib import Path

import pytest

from logistics_ops.domain.entities.dataset_asset import DatasetAsset
from logistics_ops.domain.ports.dataset_source import DatasetSource


class InMemoryDatasetSource(DatasetSource):
    def __init__(self, assets: list[DatasetAsset]) -> None:
        self._assets = assets

    def list_assets(self) -> list[DatasetAsset]:
        return self._assets


@pytest.fixture
def workspace_tmp_dir() -> Path:
    root = Path(".tmp") / "test-artifacts" / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def sample_assets(workspace_tmp_dir: Path) -> list[DatasetAsset]:
    driver_file = workspace_tmp_dir / "drivers.csv"
    driver_file.write_text("driver_id,name\nDRV001,Ana\n", encoding="utf-8")

    nested_dir = workspace_tmp_dir / "nested"
    nested_dir.mkdir()
    schema_file = nested_dir / "DATABASE_SCHEMA.txt"
    schema_file.write_text("schema content", encoding="utf-8")

    return [
        DatasetAsset(
            relative_path="drivers.csv",
            local_path=driver_file,
            content_type="text/csv",
        ),
        DatasetAsset(
            relative_path="nested/DATABASE_SCHEMA.txt",
            local_path=schema_file,
            content_type="text/plain",
        ),
    ]
