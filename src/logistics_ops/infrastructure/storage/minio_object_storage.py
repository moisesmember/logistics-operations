import logging
from pathlib import Path

from minio import Minio
from minio.error import S3Error
from urllib3.exceptions import HTTPError

from logistics_ops.domain.ports.object_storage import ObjectStorage
from logistics_ops.exceptions import DestinationAccessError, DestinationUnavailableError

logger = logging.getLogger(__name__)


class MinioObjectStorage(ObjectStorage):
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool,
    ) -> None:
        self._client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket(self, bucket: str) -> None:
        logger.info("Ensuring MinIO bucket '%s' exists.", bucket)
        try:
            if not self._client.bucket_exists(bucket):
                logger.info("Creating MinIO bucket '%s'.", bucket)
                self._client.make_bucket(bucket)
            else:
                logger.info("MinIO bucket '%s' already exists.", bucket)
        except (OSError, HTTPError) as exc:
            logger.exception("MinIO is unavailable while ensuring bucket '%s'.", bucket)
            raise DestinationUnavailableError() from exc
        except S3Error as exc:
            logger.exception("Failed to ensure bucket '%s' on MinIO.", bucket)
            raise DestinationAccessError(
                f"Falha ao acessar o MinIO durante a validacao do bucket '{bucket}'."
            ) from exc

    def object_exists(self, bucket: str, object_name: str) -> bool:
        try:
            self._client.stat_object(bucket, object_name)
            logger.debug("Object '%s' found in bucket '%s'.", object_name, bucket)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                logger.debug("Object '%s' not found in bucket '%s'.", object_name, bucket)
                return False
            logger.exception(
                "Failed to check existence of object '%s' in bucket '%s'.",
                object_name,
                bucket,
            )
            raise DestinationAccessError(
                f"Falha ao acessar o MinIO durante a consulta do objeto '{object_name}'."
            ) from exc
        except (OSError, HTTPError) as exc:
            logger.exception(
                "MinIO is unavailable while checking object '%s' in bucket '%s'.",
                object_name,
                bucket,
            )
            raise DestinationUnavailableError() from exc

    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: Path,
        content_type: str | None = None,
    ) -> None:
        kwargs = {}
        if content_type:
            kwargs["content_type"] = content_type

        logger.info(
            "Uploading local file '%s' to object '%s' in bucket '%s'.",
            file_path,
            object_name,
            bucket,
        )
        try:
            self._client.fput_object(
                bucket_name=bucket,
                object_name=object_name,
                file_path=str(file_path),
                **kwargs,
            )
        except (OSError, HTTPError) as exc:
            logger.exception(
                "MinIO is unavailable while uploading object '%s' to bucket '%s'.",
                object_name,
                bucket,
            )
            raise DestinationUnavailableError() from exc
        except S3Error as exc:
            logger.exception(
                "Failed to upload object '%s' to bucket '%s'.",
                object_name,
                bucket,
            )
            raise DestinationAccessError(
                f"Falha ao acessar o MinIO durante o upload do objeto '{object_name}'."
            ) from exc

    def get_object_bytes(self, bucket: str, object_name: str) -> bytes:
        logger.info("Reading object '%s' from bucket '%s'.", object_name, bucket)
        try:
            response = self._client.get_object(bucket, object_name)
        except (OSError, HTTPError) as exc:
            logger.exception(
                "MinIO is unavailable while reading object '%s' from bucket '%s'.",
                object_name,
                bucket,
            )
            raise DestinationUnavailableError() from exc
        except S3Error as exc:
            logger.exception(
                "Failed to read object '%s' from bucket '%s'.",
                object_name,
                bucket,
            )
            raise DestinationAccessError(
                f"Falha ao acessar o MinIO durante a leitura do objeto '{object_name}'."
            ) from exc
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        logger.info("Listing objects in bucket '%s' with prefix '%s'.", bucket, prefix)
        try:
            return [
                item.object_name
                for item in self._client.list_objects(bucket, prefix=prefix, recursive=True)
            ]
        except (OSError, HTTPError) as exc:
            logger.exception(
                "MinIO is unavailable while listing objects in bucket '%s' with prefix '%s'.",
                bucket,
                prefix,
            )
            raise DestinationUnavailableError() from exc
        except S3Error as exc:
            logger.exception(
                "Failed to list objects in bucket '%s' with prefix '%s'.",
                bucket,
                prefix,
            )
            raise DestinationAccessError(
                f"Falha ao acessar o MinIO durante a listagem do prefixo '{prefix}'."
            ) from exc
