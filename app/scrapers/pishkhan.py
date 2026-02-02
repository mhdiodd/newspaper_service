import time
import re
import hashlib
import requests
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime
from bs4 import BeautifulSoup

from app.scrapers.base import BaseScraper
from app.services.redis_client import RedisClient
from app.services.image_builder import build_cover_png
from app.utils.logger import logger


class PishkhanScraper(BaseScraper):
    agency = "pishkhan"
    multi_issue = True
    BASE_URL = "https://www.pishkhan.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/pdf",
        })
        self.redis = RedisClient()

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    def _get_today_shamsi(self) -> str:
        r = self.session.get(f"{self.BASE_URL}/all", timeout=(5,20))
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        date_tag = soup.select_one(".mash-list-items.right p")
        if not date_tag:
            raise RuntimeError("Cannot detect Shamsi date")

        digits = re.findall(r"\d+", date_tag.text)
        return "".join(digits)

    def _today_gregorian(self) -> str:
        return datetime.utcnow().strftime("%Y-%m-%d")

    def _hash(self, value: str) -> str:
        return hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]

    # --------------------------------------------------
    # Runner-level lock ID
    # --------------------------------------------------
    def get_issue_id(self) -> str:
        today = datetime.utcnow().strftime("%Y%m%d")
        issue_id = f"{self.agency}-{today}"
        logger.info("Pishkhan daily issue_id: %s", issue_id)
        return issue_id

    # --------------------------------------------------
    # Collect viewer links
    # --------------------------------------------------
    def _collect_viewers(self, shamsi_date: str) -> list[str]:
        r = self.session.get(f"{self.BASE_URL}/all", timeout=(5,20))
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        date_tag = soup.select_one(".mash-list-items.right p")
        if not date_tag:
            return []

        text = date_tag.text.strip()
        parts = text.replace("روزنامه‌های", "").strip().split()
        today_text = f"{parts[0]} {parts[1]}"

        viewers = set()

        for section in soup.select("div.section"):
            title = section.select_one("h3.section-title")
            if not title or today_text not in title.text:
                continue

            for a in section.select("a[title='دانلود پی‌دی‌اف']:not(.IconCTL)"):
                viewers.add(urljoin(self.BASE_URL, a["href"]))

        logger.info("Collected %d viewer links (%s)", len(viewers), today_text)
        return list(viewers)

    # --------------------------------------------------
    # Extract real PDF URL
    # --------------------------------------------------
    def _extract_pdf(self, viewer_url: str):
        r = self.session.get(viewer_url, timeout=(5, 20))
        r.raise_for_status()
        text = r.text

        match_paper = re.search(r"pdfviewer\.php\?paper=([^&]+)", viewer_url)
        if not match_paper:
            return None

        paper_name = match_paper.group(1)

        paper = re.search(r"paper\s*:\s*(\d+)", text)
        issue = re.search(r"id\s*:\s*(\d+)", text)
        date = re.search(r"date\s*:\s*'?(\d{8})'?", text)

        if not all([paper, issue, date]):
            return None

        payload = {
            "date": date.group(1),
            "paper": paper.group(1),
            "id": issue.group(1),
        }

        resp = self.session.post(
            f"{self.BASE_URL}/tools/PDFFiles/PDFFiles.php",
            data=payload,
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=(5,20),
        )

        pdf_rel_path = resp.text.strip()
        if not pdf_rel_path or pdf_rel_path == "null":
            return None

        return paper_name, date.group(1), urljoin(self.BASE_URL, pdf_rel_path)

    # --------------------------------------------------
    # Core download (with website-level try/except)
    # --------------------------------------------------
    def download(self, temp_dir: Path) -> Path:
        try:
            shamsi_date = self._get_today_shamsi()
            gregorian_date = self._today_gregorian()
            viewers = self._collect_viewers(shamsi_date)

            output_root = Path("/app/output/data") / self.agency
            output_root.mkdir(parents=True, exist_ok=True)

            downloaded = 0

            for viewer in viewers:
                result = self._extract_pdf(viewer)
                if not result:
                    continue

                paper, pdf_shamsi_date, pdf_url = result
                pdf_issue_id = f"{paper}:{pdf_shamsi_date}:{self._hash(pdf_url)}"

                if self.redis.is_downloaded(self.agency, pdf_issue_id):
                    continue

                paper_dir = output_root / paper / gregorian_date
                paper_dir.mkdir(parents=True, exist_ok=True)

                ts = int(time.time())
                pdf_path = paper_dir / f"{self.agency}-{ts}.pdf"
                png_path = paper_dir / f"{self.agency}-{ts}.png"

                try:
                    r = self.session.get(pdf_url, timeout=120)
                    r.raise_for_status()

                    if not r.content.startswith(b"%PDF"):
                        continue

                    pdf_path.write_bytes(r.content)

                    #  build cover
                    try:
                        build_cover_png(
                            pdf_path=pdf_path,
                            output_png=png_path,
                            dpi=200,
                        )
                    except Exception as e:
                        logger.warning("Cover failed: %s (%s)", pdf_path, e)

                except Exception as e:
                    logger.warning("Download failed: %s (%s)", pdf_url, e)
                    continue

                self.redis.record_download(
                    agency=self.agency,
                    issue_no=pdf_issue_id,
                    payload={
                        "paper": paper,
                        "shamsi_date": pdf_shamsi_date,
                        "pdf": str(pdf_path),
                        "png": str(png_path) if png_path.exists() else None,
                        "timestamp": ts,
                    },
                )

                downloaded += 1
                logger.info("Saved PDF: %s", pdf_path)

            if downloaded == 0:
                logger.warning("No new PDFs from Pishkhan")

        except Exception:
            # Website / network / structure protection
            logger.exception("Pishkhan scraper failed due to website or network issue")

        finally:
            done_file = temp_dir / "pishkhan.done"
            done_file.write_text("OK")
            return done_file
