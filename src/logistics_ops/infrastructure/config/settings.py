import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _to_bool(raw_value: str, default: bool = False) -> bool:
    value = raw_value.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass(frozen=True, slots=True)
class AppSettings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    minio_bucket: str
    minio_dataset_prefix: str
    kaggle_dataset_handle: str
    kagglehub_cache: Path

    @classmethod
    def from_env(cls) -> "AppSettings":
        load_dotenv()

        cache_value = os.getenv("KAGGLEHUB_CACHE", ".cache/kagglehub")
        cache_path = Path(cache_value).resolve()
        cache_path.mkdir(parents=True, exist_ok=True)
        os.environ["KAGGLEHUB_CACHE"] = str(cache_path)

        return cls(
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            minio_secure=_to_bool(os.getenv("MINIO_SECURE", "false")),
            minio_bucket=os.getenv("MINIO_BUCKET", "logistics-lake"),
            minio_dataset_prefix=os.getenv(
                "MINIO_DATASET_PREFIX",
                "raw/kaggle/yogape/logistics-operations-database",
            ).strip("/"),
            kaggle_dataset_handle=os.getenv(
                "KAGGLE_DATASET_HANDLE",
                "yogape/logistics-operations-database",
            ),
            kagglehub_cache=cache_path,
        )
