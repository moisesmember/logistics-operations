from fastapi.testclient import TestClient

from logistics_ops.api.app import create_app
from logistics_ops.api.dependencies import get_sync_use_case
from logistics_ops.application.dto.ingestion_result import IngestionResult
from logistics_ops.exceptions import DestinationUnavailableError


class StubSyncUseCase:
    def execute(self) -> IngestionResult:
        return IngestionResult(
            bucket="logistics-lake",
            prefix="raw/kaggle/yogape/logistics-operations-database",
            total_files=15,
            uploaded_files=10,
            skipped_files=5,
        )


class FailingSyncUseCase:
    def execute(self) -> IngestionResult:
        raise DestinationUnavailableError()


def test_healthcheck_returns_ok() -> None:
    client = TestClient(create_app())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_sync_route_executes_ingestion_use_case() -> None:
    app = create_app()
    app.dependency_overrides[get_sync_use_case] = lambda: StubSyncUseCase()
    client = TestClient(app)

    response = client.post("/api/v1/ingestions/logistics-dataset/sync")

    assert response.status_code == 200
    assert response.json() == {
        "bucket": "logistics-lake",
        "prefix": "raw/kaggle/yogape/logistics-operations-database",
        "total_files": 15,
        "uploaded_files": 10,
        "skipped_files": 5,
    }


def test_sync_route_returns_503_when_minio_is_unavailable() -> None:
    app = create_app()
    app.dependency_overrides[get_sync_use_case] = lambda: FailingSyncUseCase()
    client = TestClient(app)

    response = client.post("/api/v1/ingestions/logistics-dataset/sync")

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Nao ha fonte de destino disponivel para a ingestao. Suba o MinIO e tente novamente.",
        "path": "/api/v1/ingestions/logistics-dataset/sync",
    }
