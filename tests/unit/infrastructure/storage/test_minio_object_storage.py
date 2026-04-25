from unittest.mock import Mock

import pytest
from minio.error import S3Error

from logistics_ops.exceptions import DestinationAccessError, DestinationUnavailableError
from logistics_ops.infrastructure.storage.minio_object_storage import MinioObjectStorage


def build_storage_with_client(client: Mock) -> MinioObjectStorage:
    storage = MinioObjectStorage(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False,
    )
    storage._client = client
    return storage


def test_ensure_bucket_raises_destination_unavailable_when_minio_is_offline() -> None:
    client = Mock()
    client.bucket_exists.side_effect = OSError("connection refused")
    storage = build_storage_with_client(client)

    with pytest.raises(DestinationUnavailableError) as exc_info:
        storage.ensure_bucket("logistics-lake")

    assert "Nao ha fonte de destino disponivel" in str(exc_info.value)


def test_object_exists_raises_destination_access_error_for_unexpected_s3_error() -> None:
    client = Mock()
    client.stat_object.side_effect = S3Error(
        code="AccessDenied",
        message="denied",
        resource="/drivers.csv",
        request_id="req-1",
        host_id="host-1",
        response=None,
    )
    storage = build_storage_with_client(client)

    with pytest.raises(DestinationAccessError):
        storage.object_exists("logistics-lake", "raw/drivers.csv")
