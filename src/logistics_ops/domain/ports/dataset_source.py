from abc import ABC, abstractmethod

from logistics_ops.domain.entities.dataset_asset import DatasetAsset


class DatasetSource(ABC):
    @abstractmethod
    def list_assets(self) -> list[DatasetAsset]:
        raise NotImplementedError
