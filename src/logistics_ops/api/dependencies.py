from logistics_ops.application.use_cases.sync_dataset_to_object_storage import (
    SyncDatasetToObjectStorage,
)
from logistics_ops.bootstrap import build_sync_use_case


def get_sync_use_case() -> SyncDatasetToObjectStorage:
    return build_sync_use_case()
