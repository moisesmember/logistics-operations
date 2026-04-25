from fastapi.testclient import TestClient

from logistics_ops.api.app import create_app
from logistics_ops.api.dependencies import get_sync_use_case
from logistics_ops.application.dto.ingestion_result import IngestionResult


class StubSyncUseCase:
    def execute(self) -> IngestionResult:
        return IngestionResult(
            bucket="logistics-lake",
            prefix="raw/kaggle/yogape/logistics-operations-database",
            total_files=15,
            uploaded_files=10,
            skipped_files=5,
        )


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
