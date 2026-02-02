import json
import time
import redis
from contextlib import contextmanager
from typing import Optional, Dict

from app.config import (
    REDIS_HOST,
    REDIS_PORT,
    DOWNLOAD_TTL_DAYS,
)
from app.utils.logger import logger


class RedisClient:
    def __init__(self):
        try:
            self.r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
            )
            self.r.ping()
            logger.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)
        except Exception:
            logger.exception("Failed to connect to Redis")
            raise

    # --------------------------------------------------
    # Key builders
    # --------------------------------------------------
    def _download_key(self, agency: str, issue_no: str) -> str:
        return f"downloaded:{agency}:{issue_no}"

    def _lock_key(self, agency: str, issue_no: str) -> str:
        return f"lock:{agency}:{issue_no}"

    # --------------------------------------------------
    # Dedup check
    # --------------------------------------------------
    def is_downloaded(
        self,
        agency: str,
        issue_no: str,
    ) -> Optional[Dict]:
        try:
            key = self._download_key(agency, issue_no)
            value = self.r.get(key)

            if not value:
                return None

            return json.loads(value)

        except Exception:
            logger.exception(
                "Failed to check download status for %s issue %s",
                agency,
                issue_no,
            )
            raise

    # --------------------------------------------------
    # Distributed lock (CORRECT Context Manager)
    # --------------------------------------------------
    @contextmanager
    def acquire_lock(
        self,
        agency: str,
        issue_no: str,
        ttl: int = 300,
    ):
        """
        Redis distributed lock using SET NX EX.
        Yields True if lock is acquired, False otherwise.
        """
        key = self._lock_key(agency, issue_no)
        acquired = False

        try:
            acquired = self.r.set(
                key,
                str(int(time.time())),
                nx=True,   # only set if key does not exist
                ex=ttl,    # auto expire
            )

            if acquired:
                logger.info(
                    "Redis lock acquired for %s issue %s",
                    agency,
                    issue_no,
                )
            else:
                logger.warning(
                    "Redis lock exists, skipping: %s / %s",
                    agency,
                    issue_no,
                )

            yield bool(acquired)

        finally:
            #  release only if this process acquired the lock
            if acquired:
                try:
                    self.r.delete(key)
                    logger.info(
                        "Redis lock released for %s issue %s",
                        agency,
                        issue_no,
                    )
                except Exception:
                    logger.exception(
                        "Failed to release Redis lock for %s issue %s",
                        agency,
                        issue_no,
                    )

    # --------------------------------------------------
    # Record successful download
    # --------------------------------------------------
    def record_download(
        self,
        agency: str,
        issue_no: str,
        payload: Dict,
    ) -> None:
        try:
            key = self._download_key(agency, issue_no)

            self.r.setex(
                key,
                DOWNLOAD_TTL_DAYS * 86400,
                json.dumps(payload),
            )

            logger.info(
                "Recorded download in Redis for %s issue %s",
                agency,
                issue_no,
            )

        except Exception:
            logger.exception(
                "Failed to record download for %s issue %s",
                agency,
                issue_no,
            )
            raise
