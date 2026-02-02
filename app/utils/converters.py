import zipfile
from pathlib import Path

from app.utils.logger import logger


def extract_files_from_zip(zip_path: Path, output_dir: Path) -> list[Path]:
    """
    Extract ZIP and return all files inside it (recursive).
    """
    try:
        logger.info("Starting ZIP extraction: %s", zip_path)

        output_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(output_dir)

        files = sorted(
            [p for p in output_dir.rglob("*") if p.is_file()],
            key=lambda p: p.name
        )

        if not files:
            raise RuntimeError("ZIP extracted but no files found")

        logger.info(
            "ZIP extraction completed successfully. %d files found.",
            len(files),
        )

        return files

    except zipfile.BadZipFile:
        logger.exception("Invalid or corrupted ZIP file: %s", zip_path)
        raise

    except Exception:
        logger.exception(
            "Failed to extract files from ZIP: %s",
            zip_path,
        )
        raise
