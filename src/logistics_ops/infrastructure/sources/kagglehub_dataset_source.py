import mimetypes
from pathlib import Path

import kagglehub

from logistics_ops.domain.entities.dataset_asset import DatasetAsset
from logistics_ops.domain.ports.dataset_source import DatasetSource


class KaggleHubDatasetSource(DatasetSource):
    """Downloads a Kaggle dataset and exposes its files as domain assets."""

    def __init__(self, dataset_handle: str, cache_dir: Path) -> None:
        self._dataset_handle = dataset_handle
        self._cache_dir = cache_dir

    def list_assets(self) -> list[DatasetAsset]:
        dataset_root = Path(
            kagglehub.dataset_download(
                self._dataset_handle,
                output_dir=str(self._cache_dir),
            )
        ).resolve()

        assets: list[DatasetAsset] = []
        for file_path in sorted(path for path in dataset_root.rglob("*") if path.is_file()):
            relative_path = file_path.relative_to(dataset_root).as_posix()
            content_type, _ = mimetypes.guess_type(file_path.name)
            assets.append(
                DatasetAsset(
                    relative_path=relative_path,
                    local_path=file_path,
                    content_type=content_type,
                )
            )
        return assets
