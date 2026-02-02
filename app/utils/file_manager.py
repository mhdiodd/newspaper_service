import shutil
from pathlib import Path

from app.config import TEMP_DIR
from app.utils.logger import logger


def cleanup_temp(keep_dirs: list[Path] | None = None):
    """
    Remove temporary working directories but keep final outputs.
    """
    try:
        keep_dirs = keep_dirs or []

        if not TEMP_DIR.exists():
            logger.info("TEMP_DIR does not exist, nothing to clean")
            return

        logger.info("Starting cleanup of TEMP_DIR: %s", TEMP_DIR)

        for item in TEMP_DIR.iterdir():
            # Keep final output directories
            if any(item.resolve() == k.resolve() for k in keep_dirs):
                logger.info("Skipping kept directory: %s", item)
                continue

            if item.is_dir():
                logger.info("Removing directory: %s", item)
                shutil.rmtree(item)
            else:
                logger.info("Removing file: %s", item)
                item.unlink()

        logger.info("TEMP_DIR cleanup completed successfully")

    except Exception:
        logger.exception("Failed during TEMP_DIR cleanup")
        raise
