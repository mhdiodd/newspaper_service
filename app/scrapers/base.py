from abc import ABC
from pathlib import Path


class BaseScraper(ABC):
    """
    Base contract for all newspaper scrapers.
    Runner only talks to this interface.
    """

    agency: str  # e.g. "iran", "etemad"
    multi_issue: bool = False  # important for runner logic

    def get_issue_id(self) -> str:
        """
        Return unique issue identifier (used for deduplication).
        """
        raise NotImplementedError

    def download(self, temp_dir: Path) -> Path:
        """
        Download issue(s).
        For single-issue scrapers:
            must return path to final PDF inside temp_dir
        For multi-issue scrapers (like pishkhan):
            can return a dummy file (runner must skip PDF handling)
        """
        raise NotImplementedError
