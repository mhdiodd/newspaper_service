from pathlib import Path
from typing import Optional
import logging

from minio import Minio
from minio.error import S3Error

from app import config

logger = logging.getLogger(__name__)


class StorageBackend:
    def save(self, local_path: Path, remote_path: str) -> Optional[str]:
        """
        Save a file to storage backend.
        Returns final URI if successful.
        """
        raise NotImplementedError


class LocalStorage(StorageBackend):
    def save(self, local_path: Path, remote_path: str) -> str:
        # File already exists locally; return local URI
        logger.debug(f"LocalStorage: using local file {local_path}")
        return f"file://{local_path}"


class MinIOStorage(StorageBackend):
    def __init__(self) -> None:
        self.client = Minio(
            endpoint=config.S3_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=config.S3_ACCESS_KEY,
            secret_key=config.S3_SECRET_KEY,
            secure=config.S3_SECURE,
        )

        self.bucket = config.S3_BUCKET
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        """
        Ensure bucket exists.
        """
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"MinIO bucket created: {self.bucket}")
        except S3Error as exc:
            logger.exception(f"Failed to ensure bucket {self.bucket}: {exc}")
            raise

    def save(self, local_path: Path, remote_path: str) -> Optional[str]:
        try:
            self.client.fput_object(
                bucket_name=self.bucket,
                object_name=remote_path,
                file_path=str(local_path),
            )

            uri = f"s3://{self.bucket}/{remote_path}"
            logger.info(f"MinIO upload successful: {uri}")
            return uri

        except S3Error as exc:
            logger.exception(f"MinIO upload failed for {local_path}: {exc}")
            return None


class CompositeStorage(StorageBackend):
    def __init__(self) -> None:
        self.local = LocalStorage()
        self.remote = MinIOStorage()

    def save(self, local_path: Path, remote_path: str) -> Optional[str]:
        # Always keep local copy
        self.local.save(local_path, remote_path)

        # Best-effort remote upload
        return self.remote.save(local_path, remote_path)
