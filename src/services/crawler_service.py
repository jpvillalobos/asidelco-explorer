"""
Crawler Service with Progress Reporting
"""
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict

logger = logging.getLogger(__name__)


class CrawlerService:
    """Service for web crawling with progress reporting"""

    def __init__(self):
        """Initialize crawler service"""
        self.session = requests.Session()
        self.headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9,es;q=0.8,es-ES;q=0.7",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "x-microsoftajax": "Delta=true",
            "x-requested-with": "XMLHttpRequest"
        }

    def crawl_projects(
        self,
        base_url: str,
        project_url: Optional[str] = None,
        input_file: Optional[str] = None,
        output_dir: Optional[str] = None,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Crawl project pages from CFIA

        Args:
            base_url: Base URL for the CFIA service
            project_url: Specific project URL template
            input_file: Path to Excel file with project IDs
            output_dir: Directory to save HTML files
            context: Optional context for progress reporting

        Returns:
            Dictionary with crawl results
        """
        logger.info(f"Starting project crawl: {base_url}")

        # Resolve paths
        if not output_dir:
            output_dir = Path.cwd() / "data" / "output" / "projects" / "html"
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Load project IDs
        project_ids = []
        if input_file:
            logger.info(f"Loading project IDs from: {input_file}")

            # Determine file type and read accordingly
            input_path = Path(input_file)
            if input_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(input_file)
            elif input_path.suffix.lower() == '.csv':
                df = pd.read_csv(input_file)
            else:
                # Try CSV first, fallback to Excel
                try:
                    df = pd.read_csv(input_file)
                except Exception:
                    df = pd.read_excel(input_file)

            if "Proyecto" in df.columns:
                project_ids = df["Proyecto"].dropna().tolist()
            elif "proyecto" in df.columns:
                project_ids = df["proyecto"].dropna().tolist()
            else:
                # Try first column
                project_ids = df.iloc[:, 0].dropna().tolist()
            logger.info(f"Loaded {len(project_ids)} project IDs")

        # Initialize form state
        url = project_url or f"{base_url}/ConsultaProyecto/"
        self.headers["origin"] = base_url
        self.headers["referer"] = url

        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            viewstate = soup.select_one("#__VIEWSTATE")["value"]
            eventvalidation = soup.select_one("#__EVENTVALIDATION")["value"]
            viewstategen = soup.select_one("#__VIEWSTATEGENERATOR")["value"]
        except Exception as e:
            logger.error(f"Failed to initialize form state: {e}")
            return {
                "count": 0,
                "output_dir": str(output_dir),
                "error": str(e)
            }

        # Track results
        seen_ids = defaultdict(int)
        success_count = 0
        error_count = 0
        total_projects = len(project_ids)

        # Crawl each project
        for index, pid in enumerate(project_ids, start=1):
            try:
                pid = str(int(pid))
            except (ValueError, TypeError):
                logger.warning(f"Invalid project ID format: {pid}")
                error_count += 1
                continue

            # Update progress
            if context:
                context.report_progress(
                    index,
                    total_projects,
                    f"Crawling project {pid} ({index}/{total_projects})",
                    {"success": success_count, "errors": error_count}
                )

            # Handle duplicate IDs
            seen_ids[pid] += 1
            suffix = f"_{seen_ids[pid]}" if seen_ids[pid] > 1 else ""
            filename = output_dir / f"{pid}{suffix}.html"

            # Prepare payload
            payload = {
                "ScriptManager1": "UpdatePanel1|btnConsultar",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": viewstate,
                "__VIEWSTATEGENERATOR": viewstategen,
                "__EVENTVALIDATION": eventvalidation,
                "identificadores": "radioNumProyecto",
                "txtValor": pid,
                "__ASYNCPOST": "true",
                "btnConsultar": "Consultar"
            }

            # Attempt request with retries
            max_retries = 3
            attempt = 0
            success = False

            while attempt < max_retries and not success:
                attempt += 1
                try:
                    response = self.session.post(url, data=payload, headers=self.headers, timeout=30)

                    if response.status_code == 200:
                        with open(filename, "w", encoding="utf-8") as f:
                            f.write(response.text)
                        logger.debug(f"Saved project {pid} to {filename}")
                        success_count += 1
                        success = True
                    else:
                        logger.warning(f"HTTP {response.status_code} for project {pid}")

                except Exception as e:
                    logger.error(f"Attempt {attempt} failed for project {pid}: {e}")
                    if attempt < max_retries:
                        time.sleep(2 ** attempt)  # Exponential backoff

            if not success:
                error_count += 1
                logger.error(f"Failed to crawl project {pid} after {max_retries} attempts")

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Project crawl completed: {success_count} success, {error_count} errors")

        return {
            "count": success_count,
            "output_dir": str(output_dir),
            "total_projects": total_projects,
            "errors": error_count
        }

    def crawl_professionals(
        self,
        base_url: str,
        directory_url: str,
        max_members: int = 100,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Crawl professional profiles from CFIA

        Args:
            base_url: Base URL for the CFIA service
            directory_url: URL for the members directory
            max_members: Maximum number of members to crawl
            input_dir: Directory with project HTML files to extract carnets
            output_dir: Directory to save professional HTML/JSON files
            context: Optional context for progress reporting

        Returns:
            Dictionary with crawl results
        """
        logger.info(f"Starting professionals crawl: {directory_url}")

        # Resolve paths
        if not output_dir:
            output_dir = Path.cwd() / "data" / "output" / "professionals"
        else:
            output_dir = Path(output_dir)

        output_html_dir = output_dir / "html"
        output_json_dir = output_dir / "json"
        output_html_dir.mkdir(parents=True, exist_ok=True)
        output_json_dir.mkdir(parents=True, exist_ok=True)

        # TODO: Extract carnets from project HTML files
        # This would require parsing the project HTML files and extracting professional IDs
        # For now, return placeholder

        logger.info("Professional crawling not fully implemented yet")

        return {
            "count": 0,
            "output_dir": str(output_dir),
            "message": "Professional crawling requires project HTML parsing implementation"
        }
