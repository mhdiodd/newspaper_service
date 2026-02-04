import logging
import time
from pathlib import Path

from app.services.redis_client import RedisClient
from app.services.image_builder import build_cover_png
from app.services.object_storage import CompositeStorage
from app.utils.file_manager import cleanup_temp

logger = logging.getLogger(__name__)


def run(scraper, agency: str, base_dir: Path):
    redis = RedisClient()
    storage = CompositeStorage()

    tmp_root = base_dir / "tmp"
    data_root = base_dir / "data"

    issue_id = scraper.get_issue_id()
    logger.info("Starting scraper: agency=%s, issue_id=%s", agency, issue_id)

    try:
        with redis.acquire_lock(
            agency=agency,
            issue_no=issue_id,
            ttl=60 * 60,
        ) as acquired:

            if not acquired:
                logger.warning("Lock exists, skipping: %s / %s", agency, issue_id)
                return

            if not getattr(scraper, "multi_issue", False):
                if redis.is_downloaded(agency, issue_id):
                    logger.info("Already processed: %s / %s", agency, issue_id)
                    return

            temp_dir = tmp_root / agency / issue_id
            temp_dir.mkdir(parents=True, exist_ok=True)

            logger.info(
                "Running scraper (multi_issue=%s)",
                getattr(scraper, "multi_issue", False),
            )

            result = scraper.download(temp_dir)

            # ---------------- MULTI ISSUE ----------------
            # (e.g. Pishkhan – handled inside scraper)
            if getattr(scraper, "multi_issue", False):
                logger.info("Multi-issue scraper finished successfully")
                return

            # ---------------- SINGLE ISSUE ----------------
            if not isinstance(result, Path) or not result.exists():
                logger.error("Invalid single-issue result")
                return

            today = time.strftime("%Y-%m-%d")
            final_dir = data_root / agency / today
            final_dir.mkdir(parents=True, exist_ok=True)

            ts = int(time.time())

            final_pdf = final_dir / f"{agency}-{ts}.pdf"
            final_png = final_dir / f"{agency}-{ts}.png"

            # Move PDF to final location
            result.replace(final_pdf)

            # Build PNG cover
            try:
                build_cover_png(
                    pdf_path=final_pdf,
                    output_png=final_png,
                    dpi=200,
                )
            except Exception:
                logger.exception("Cover generation failed")

            # -------- STORAGE (LOCAL + S3) --------
            pdf_remote_key = f"{agency}/{today}/{final_pdf.name}"
            png_remote_key = f"{agency}/{today}/{final_png.name}"

            pdf_uri = storage.save(final_pdf, pdf_remote_key)
            png_uri = None

            if final_png.exists():
                png_uri = storage.save(final_png, png_remote_key)

            # -------- REDIS METADATA --------
            redis.record_download(
                agency=agency,
                issue_no=issue_id,
                payload={
                    "pdf": {
                        "local": str(final_pdf),
                        "remote": pdf_uri,
                    },
                    "png": {
                        "local": str(final_png) if final_png.exists() else None,
                        "remote": png_uri,
                    },
                    "timestamp": ts,
                },
            )

            logger.info(
                "Single issue processed successfully: agency=%s issue_id=%s",
                agency,
                issue_id,
            )

    finally:
        # -------- CLEAN TEMP --------
        try:
            cleanup_temp()
        except Exception:
            logger.warning("Temp cleanup failed")
