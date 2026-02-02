from pathlib import Path
from PyPDF2 import PdfMerger

from app.utils.logger import logger


def merge_pdfs(pdf_files: list[Path], output_pdf: Path) -> None:
    merger = None

    try:
        if not pdf_files:
            raise ValueError("PDF list is empty")

        output_pdf.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Merging %d PDFs into %s",
            len(pdf_files),
            output_pdf,
        )

        merger = PdfMerger()

        for pdf in pdf_files:
            logger.info("Appending PDF: %s", pdf)
            merger.append(str(pdf))

        merger.write(str(output_pdf))

        logger.info("Final merged PDF created: %s", output_pdf)

    except Exception:
        logger.exception(
            "Failed to merge PDFs into %s",
            output_pdf,
        )
        raise

    finally:
        if merger is not None:
            try:
                merger.close()
            except Exception:
                logger.exception("Failed to close PdfMerger")
