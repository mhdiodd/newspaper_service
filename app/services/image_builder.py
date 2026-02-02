from pathlib import Path
import fitz  # PyMuPDF

from app.utils.logger import logger


def build_cover_png(pdf_path: Path, output_png: Path, dpi: int = 200) -> None:
    """
    Extract first page of PDF and save as PNG using PyMuPDF.
    Does NOT require poppler or system dependencies.
    """
    doc = None

    try:
        logger.info("Building cover PNG from PDF: %s", pdf_path)

        doc = fitz.open(pdf_path)

        if doc.page_count == 0:
            raise RuntimeError("PDF has no pages")

        page = doc.load_page(0)

        # Convert DPI to zoom factor (72 is default PDF DPI)
        zoom = dpi / 72
        matrix = fitz.Matrix(zoom, zoom)

        pix = page.get_pixmap(matrix=matrix)

        output_png.parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(output_png))

        logger.info("Cover PNG created successfully: %s", output_png)

    except Exception:
        logger.exception(
            "Failed to build cover PNG from PDF: %s",
            pdf_path,
        )
        raise

    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                logger.exception("Failed to close PDF document: %s", pdf_path)
