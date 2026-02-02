import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path

from app.scrapers.base import BaseScraper
from app.utils.converters import extract_files_from_zip
from app.services.pdf_builder import merge_pdfs
from app.utils.logger import logger


class EtemadScraper(BaseScraper):
    agency = "etemad"

    BASE_URL = "https://www.etemadnewspaper.ir"
    DOWNLOAD_ENDPOINT = "/fa/download-pages"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Referer": self.BASE_URL,
            }
        )

    def fetch_homepage(self) -> str:
        try:
            response = self.session.get(self.BASE_URL, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception:
            logger.exception("Failed to fetch Etemad homepage")
            raise

    def get_issue_id(self) -> str:
        try:
            html = self.fetch_homepage()
            soup = BeautifulSoup(html, "html.parser")

            span = soup.select_one(
                "span#ContentPlaceHolder1_activedate_lblNPNNO"
            )
            if not span:
                raise RuntimeError("Issue number not found")

            text = span.get_text(strip=True)
            match = re.search(r"\d+", text)

            if not match:
                raise RuntimeError(f"Invalid issue number: {text}")

            issue_id = match.group()
            logger.info("Etemad issue_id detected: %s", issue_id)
            return issue_id

        except Exception:
            logger.exception("Failed to extract issue_id for Etemad")
            raise

    def download(self, temp_dir: Path) -> Path:
        zip_path = None
        extract_dir = None
        final_pdf = None

        try:
            html = self.fetch_homepage()
            soup = BeautifulSoup(html, "html.parser")

            container = soup.find("div", id="divcp2")
            if not container:
                raise RuntimeError("Download container not found")

            params = {
                "npn_id": container.get("data-npnid"),
                "type": container.get("data-type"),
                "page_no": container.get("data-pageno", "1"),
            }

            logger.info("Starting Etemad download with params: %s", params)

            response = self.session.post(
                f"{self.BASE_URL}{self.DOWNLOAD_ENDPOINT}",
                data=params,
                timeout=60,
            )
            response.raise_for_status()

            zip_path = temp_dir / "etemad_pages.zip"
            zip_path.write_bytes(response.content)

            logger.info("Etemad zip downloaded: %s", zip_path)

            extract_dir = temp_dir / "pages"
            files = extract_files_from_zip(zip_path, extract_dir)

            pdfs = sorted(
                file for file in files if file.suffix.lower() == ".pdf"
            )
            if not pdfs:
                raise RuntimeError("No PDFs extracted from Etemad zip")

            final_pdf = temp_dir / "etemad_final.pdf"
            merge_pdfs(pdfs, final_pdf)

            logger.info("Etemad final PDF created: %s", final_pdf)
            return final_pdf

        except Exception:
            logger.exception("Etemad download pipeline failed")
            raise
