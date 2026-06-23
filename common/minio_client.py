import json
import logging
from io import BytesIO
from typing import Any
 
from minio import Minio
from minio.error import S3Error
 
from common.logging_config import setup_logging
 
setup_logging()
logger = logging.getLogger(__name__)
 
 
class MinIOStorage:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ):
        self.client = Minio(endpoint, access_key, secret_key, secure=secure)
        self.bucket = bucket
        self._ensure_bucket()
 
    def _ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Создан бакет MinIO: {self.bucket}")
 
    def put_json(self, key: str, data: Any) -> str:
        body = json.dumps(data, default=str).encode("utf-8")
        self.client.put_object(
            self.bucket,
            key,
            BytesIO(body),
            len(body),
            content_type="application/json",
        )
        logger.info(f"Сохранено в MinIO: {self.bucket}/{key} ({len(body)} bytes)")
        return key
 
    def get_json(self, key: str) -> Any:
        try:
            response = self.client.get_object(self.bucket, key)
        except S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"Объект не найден в MinIO: {self.bucket}/{key}") from e
            raise
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()
 
    def delete(self, key: str) -> None:
        self.client.remove_object(self.bucket, key)
        logger.info(f"Удалено из MinIO: {self.bucket}/{key}")
 
    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise
