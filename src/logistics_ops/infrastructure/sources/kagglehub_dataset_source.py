import logging
import mimetypes
from pathlib import Path

import kagglehub

from logistics_ops.domain.entities.dataset_asset import DatasetAsset
from logistics_ops.domain.ports.dataset_source import DatasetSource

logger = logging.getLogger(__name__)


class KaggleHubDatasetSource(DatasetSource):
    """Downloads a Kaggle dataset and exposes its files as domain assets."""

    def __init__(self, dataset_handle: str, cache_dir: Path) -> None:
        self._dataset_handle = dataset_handle
        self._cache_dir = cache_dir

    def get_dataset_root(self) -> Path:
        logger.info(
            "Downloading or reusing Kaggle dataset '%s' into '%s'.",
            self._dataset_handle,
            self._cache_dir,
        )
        dataset_root = Path(
            kagglehub.dataset_download(
                self._dataset_handle,
                output_dir=str(self._cache_dir),
            )
        ).resolve()
        logger.info("Resolved Kaggle dataset root to '%s'.", dataset_root)
        return dataset_root

    def list_assets(self) -> list[DatasetAsset]:
        dataset_root = self.get_dataset_root()

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
        logger.info("Kaggle dataset source exposed %s files.", len(assets))
        return assets
