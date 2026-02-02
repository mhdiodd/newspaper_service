# app/config.py
from pathlib import Path

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent.parent
TEMP_DIR = BASE_DIR / "output" / "tmp"

# ---------- Redis ----------
REDIS_HOST = "newspaper_redis"   # docker-compose service name
REDIS_PORT = 6379

# ---------- Download / Dedup ----------
DOWNLOAD_TTL_DAYS = 2

# ---------- Scheduler ----------
RUN_HOURS = "0,6,12,18"
