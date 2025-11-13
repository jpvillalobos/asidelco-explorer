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
        rate_limit: float = 0.5,
        max_retries: int = 3,
        timeout: int = 30,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Crawl project pages from CFIA

        Args:
            base_url: Base URL for the CFIA service
            project_url: Specific project URL template
            input_file: Path to Excel file with project IDs
            output_dir: Directory to save HTML files
            rate_limit: Seconds to wait between requests (default: 0.5)
            max_retries: Maximum retry attempts for failed requests (default: 3)
            timeout: Request timeout in seconds (default: 30)
            context: Optional context for progress reporting

        Returns:
            Dictionary with crawl results
        """
        logger.info(f"Starting project crawl: {base_url}")
        logger.info(f"Crawler settings - Rate limit: {rate_limit}s, Max retries: {max_retries}, Timeout: {timeout}s")

        # Resolve paths
        if not output_dir:
            output_dir = Path.cwd() / "data" / "output" / "projects" / "html"
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Build hash set of already crawled project IDs from existing files
        logger.info(f"Scanning output directory for existing files: {output_dir}")
        crawled_ids = set()
        if output_dir.exists():
            existing_files = list(output_dir.glob("*.html"))
            for file in existing_files:
                # Extract project ID from filename (e.g., "12345.html" -> "12345")
                try:
                    pid = file.stem  # Get filename without extension
                    crawled_ids.add(pid)
                except Exception as e:
                    logger.warning(f"Could not parse filename {file.name}: {e}")

            logger.info(f"Found {len(crawled_ids)} already-crawled projects in output directory")
            if len(crawled_ids) > 0:
                logger.info(f"These projects will be skipped to avoid re-crawling")
        else:
            logger.info(f"Output directory is empty - no existing files to skip")

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
            logger.info(f"Loaded {len(project_ids)} project IDs from input file")

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
        success_count = 0
        error_count = 0
        skipped_count = 0
        total_projects = len(project_ids)

        logger.info(f"Starting crawl of {total_projects} projects")
        logger.info("="*80)

        # Crawl each project
        for index, pid in enumerate(project_ids, start=1):
            # Validate and normalize project ID
            try:
                pid = str(int(pid))
            except (ValueError, TypeError):
                logger.warning(f"[{index}/{total_projects}] Invalid project ID format: {pid} - skipping")
                error_count += 1
                continue

            # Check if already crawled using hash set
            if pid in crawled_ids:
                logger.info(f"[{index}/{total_projects}] Skipping project ID '{pid}' - already crawled (found in hash set)")
                skipped_count += 1
                # Update progress
                if context:
                    context.report_progress(
                        index,
                        total_projects,
                        f"Skipped project {pid} (already crawled) ({index}/{total_projects})",
                        {"success": success_count, "errors": error_count, "skipped": skipped_count}
                    )
                continue

            # Log start of crawl for this project
            logger.info(f"[{index}/{total_projects}] Crawling project ID: {pid}")

            # Update progress
            if context:
                context.report_progress(
                    index,
                    total_projects,
                    f"Crawling project {pid} ({index}/{total_projects})",
                    {"success": success_count, "errors": error_count, "skipped": skipped_count}
                )

            filename = output_dir / f"{pid}.html"

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
            attempt = 0
            success = False

            while attempt < max_retries and not success:
                attempt += 1
                try:
                    response = self.session.post(url, data=payload, headers=self.headers, timeout=timeout)

                    if response.status_code == 200:
                        with open(filename, "w", encoding="utf-8") as f:
                            f.write(response.text)

                        # Add to hash set to track as crawled
                        crawled_ids.add(pid)

                        logger.info(f"[{index}/{total_projects}] ✓ Successfully crawled project ID: {pid} → {filename.name}")
                        success_count += 1
                        success = True
                    else:
                        logger.warning(f"[{index}/{total_projects}] HTTP {response.status_code} for project {pid} (attempt {attempt}/{max_retries})")

                except Exception as e:
                    logger.error(f"[{index}/{total_projects}] Attempt {attempt}/{max_retries} failed for project {pid}: {e}")
                    if attempt < max_retries:
                        backoff_time = 2 ** attempt
                        logger.info(f"[{index}/{total_projects}] Waiting {backoff_time}s before retry (exponential backoff)...")
                        time.sleep(backoff_time)

            if not success:
                error_count += 1
                logger.error(f"[{index}/{total_projects}] ✗ Failed to crawl project {pid} after {max_retries} attempts")

            # Rate limiting
            time.sleep(rate_limit)

        # Final summary
        logger.info("="*80)
        logger.info(f"Project crawl completed")
        logger.info(f"  Total projects in input: {total_projects}")
        logger.info(f"  ✓ Successfully crawled: {success_count}")
        logger.info(f"  ↷ Skipped (already crawled): {skipped_count}")
        logger.info(f"  ✗ Errors: {error_count}")
        logger.info(f"  Output directory: {output_dir}")
        logger.info("="*80)

        return {
            "count": success_count,
            "output_dir": str(output_dir),
            "total_projects": total_projects,
            "errors": error_count,
            "skipped": skipped_count
        }

    def crawl_professionals(
        self,
        base_url: str,
        directory_url: str,
        max_members: int = 100,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        rate_limit: float = 0.5,
        max_retries: int = 3,
        timeout: int = 30,
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
            rate_limit: Seconds to wait between requests (default: 0.5)
            max_retries: Maximum retry attempts for failed requests (default: 3)
            timeout: Request timeout in seconds (default: 30)
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
