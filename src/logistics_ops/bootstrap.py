from logistics_ops.application.use_cases.sync_dataset_to_object_storage import (
    SyncDatasetToObjectStorage,
)
from logistics_ops.infrastructure.config.settings import AppSettings
from logistics_ops.infrastructure.readers.hybrid_tabular_reader import HybridTabularReader
from logistics_ops.infrastructure.readers.local_tabular_reader import LocalTabularReader
from logistics_ops.infrastructure.readers.minio_tabular_reader import MinioTabularReader
from logistics_ops.infrastructure.sources.kagglehub_dataset_source import (
    KaggleHubDatasetSource,
)
from logistics_ops.infrastructure.storage.minio_object_storage import MinioObjectStorage


def build_settings() -> AppSettings:
    return AppSettings.from_env()


def build_storage(settings: AppSettings | None = None) -> MinioObjectStorage:
    resolved_settings = settings or build_settings()
    return MinioObjectStorage(
        endpoint=resolved_settings.minio_endpoint,
        access_key=resolved_settings.minio_access_key,
        secret_key=resolved_settings.minio_secret_key,
        secure=resolved_settings.minio_secure,
    )


def build_dataset_source(settings: AppSettings | None = None) -> KaggleHubDatasetSource:
    resolved_settings = settings or build_settings()
    return KaggleHubDatasetSource(
        dataset_handle=resolved_settings.kaggle_dataset_handle,
        cache_dir=resolved_settings.kagglehub_cache,
    )


def build_sync_use_case(settings: AppSettings | None = None) -> SyncDatasetToObjectStorage:
    resolved_settings = settings or build_settings()
    return SyncDatasetToObjectStorage(
        source=build_dataset_source(resolved_settings),
        storage=build_storage(resolved_settings),
        bucket=resolved_settings.minio_bucket,
        prefix=resolved_settings.minio_dataset_prefix,
    )


def build_minio_tabular_reader(settings: AppSettings | None = None) -> MinioTabularReader:
    resolved_settings = settings or build_settings()
    return MinioTabularReader(
        storage=build_storage(resolved_settings),
        bucket=resolved_settings.minio_bucket,
        dataset_prefix=resolved_settings.minio_dataset_prefix,
    )


def build_local_tabular_reader(settings: AppSettings | None = None) -> LocalTabularReader:
    resolved_settings = settings or build_settings()
    source = build_dataset_source(resolved_settings)
    return LocalTabularReader(
        dataset_root=source.get_dataset_root(),
        dataset_prefix=resolved_settings.minio_dataset_prefix,
    )


def build_tabular_reader(settings: AppSettings | None = None) -> HybridTabularReader:
    resolved_settings = settings or build_settings()
    return HybridTabularReader(
        primary_reader=build_minio_tabular_reader(resolved_settings),
        fallback_reader=build_local_tabular_reader(resolved_settings),
    )
