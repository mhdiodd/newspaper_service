import logging
import sys
from pathlib import Path

# Application name (used in logs)
APP_NAME = "newspaper_service"

# Absolute log directory inside container
LOG_DIR = Path("/app/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "cron.log"


def setup_logger() -> logging.Logger:
    """
    Configure and return application logger.
    """

    logger = logging.getLogger(APP_NAME)
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs when imported multiple times
    if logger.handlers:
        return logger

    # ---- Formatter ----
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ---- Console handler ----
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # ---- File handler ----
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# Public logger instance
logger = setup_logger()
