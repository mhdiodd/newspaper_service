from pathlib import Path
from app.runner import run
from app.scrapers.pishkhan import PishkhanScraper
from app.scrapers.etemad import EtemadScraper
from app.scrapers.iran import IranScraper
from app.utils.logger import logger


BASE_DIR = Path("/app/output")

SCRAPERS = [
    ("etemad", EtemadScraper()),
    ("iran",IranScraper()),
    ("pishkhan",PishkhanScraper())

]


def main():
    logger.info("Starting scraper runner")

    for agency, scraper in SCRAPERS:
        scraper_name = scraper.__class__.__name__
        try:
            logger.info("Running scraper: %s", scraper_name)
            run(scraper=scraper, agency=agency, base_dir=BASE_DIR)
            logger.info("Scraper finished successfully: %s", scraper_name)
        except Exception:
            logger.exception("Scraper failed and will be skipped: %s", scraper_name)

    logger.info("All scrapers processed")


if __name__ == "__main__":
    main()
