import logging
import time
from pathlib import Path

from app.services.redis_client import RedisClient
from app.services.image_builder import build_cover_png
from app.utils.file_manager import cleanup_temp
from app.config import TEMP_DIR

logger = logging.getLogger(__name__)


def run(scraper, agency: str, base_dir: Path):
    redis = RedisClient()

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

            # Multi-issue ends here (Pishkhan)
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

            final_pdf = final_dir / f"{agency}.pdf"
            final_png = final_dir / f"{agency}.png"

            result.replace(final_pdf)

            try:
                build_cover_png(
                    pdf_path=final_pdf,
                    output_png=final_png,
                    dpi=200,
                )
            except Exception:
                logger.exception("Cover generation failed")

            redis.record_download(
                agency=agency,
                issue_no=issue_id,
                payload={
                    "pdf": final_pdf.name,
                    "png": final_png.name if final_png.exists() else None,
                    "timestamp": int(time.time()),
                },
            )

    finally:
        #  CLEAN TEMP
        try:
            cleanup_temp()
        except Exception:
            logger.warning("Temp cleanup failed")
