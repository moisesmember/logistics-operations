import os
import shutil
import uuid
from pathlib import Path

import pytest
from minio.error import S3Error

from logistics_ops.infrastructure.storage.minio_object_storage import MinioObjectStorage


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION_TESTS", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


@pytest.fixture(scope="session")
def integration_storage() -> MinioObjectStorage:
    if not _integration_enabled():
        pytest.skip("Set RUN_INTEGRATION_TESTS=1 to run integration tests.")

    storage = MinioObjectStorage(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=os.getenv("MINIO_SECURE", "false").strip().lower() == "true",
    )
    return storage


@pytest.fixture
def integration_bucket(integration_storage: MinioObjectStorage) -> str:
    bucket = f"test-logistics-{uuid.uuid4().hex[:10]}"
    integration_storage.ensure_bucket(bucket)
    yield bucket
    objects = integration_storage.list_objects(bucket, "")
    client = integration_storage._client
    for object_name in objects:
        client.remove_object(bucket, object_name)
    try:
        client.remove_bucket(bucket)
    except S3Error:
        pass


@pytest.fixture
def sample_csv_file() -> Path:
    root = Path(".tmp") / "integration-artifacts" / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    file_path = root / "drivers.csv"
    file_path.write_text("driver_id,name\nDRV001,Ana\n", encoding="utf-8")
    yield file_path
    shutil.rmtree(root, ignore_errors=True)
