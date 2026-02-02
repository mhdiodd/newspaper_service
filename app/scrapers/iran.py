import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path

from app.scrapers.base import BaseScraper
from app.utils.logger import logger


class IranScraper(BaseScraper):
    agency = "iran"
    BASE_URL = "https://irannewspaper.ir"

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
            r = self.session.get(self.BASE_URL, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception:
            logger.exception("Failed to fetch Iran homepage")
            raise

    def get_issue_id(self) -> str:
        try:
            soup = BeautifulSoup(self.fetch_homepage(), "html.parser")

            span = soup.select_one("span.title[data-title]")
            if not span:
                raise RuntimeError("Issue title not found")

            raw = span.get("data-title", "")
            digits = re.sub(r"[^\d۰-۹]", "", raw)

            if not digits:
                raise RuntimeError(f"Invalid issue id: {raw}")

            logger.info("Iran issue_id detected: %s", digits)
            return digits

        except Exception:
            logger.exception("Failed to extract issue_id for Iran")
            raise

    def download(self, temp_dir: Path) -> Path:
        try:
            soup = BeautifulSoup(self.fetch_homepage(), "html.parser")

            span = soup.find(
                "span",
                string=lambda s: s and "تمام صفحات" in s,
            )
            if not span:
                raise RuntimeError("Full PDF span not found")

            a = span.find_parent("a")
            if not a or not a.get("href"):
                raise RuntimeError("Full PDF link not found")

            url = a["href"]
            if not url.startswith("http"):
                url = self.BASE_URL + url

            logger.info("Starting Iran PDF download: %s", url)

            r = self.session.get(url, timeout=60)
            r.raise_for_status()

            pdf_path = temp_dir / "iran_final.pdf"
            with open(pdf_path, "wb") as f:
                f.write(r.content)

            logger.info("Iran final PDF saved: %s", pdf_path)
            return pdf_path

        except Exception:
            logger.exception("Iran download pipeline failed")
            raise
