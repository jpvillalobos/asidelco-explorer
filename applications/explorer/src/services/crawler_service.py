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
import re
import json

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
        Crawl professional profiles from CFIA by extracting carnets from project HTML files

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
        logger.info(f"Starting professionals crawl from: {directory_url}")
        logger.info(f"Crawler settings - Rate limit: {rate_limit}s, Max retries: {max_retries}, Timeout: {timeout}s")

        # Resolve paths - ONLY resolve filesystem paths, NOT URLs
        if not input_dir:
            input_dir = Path.cwd() / "data" / "output" / "projects" / "html"
        else:
            # Check if it's a URL (starts with http:// or https://)
            if not input_dir.startswith(('http://', 'https://')):
                input_dir = Path(input_dir)
            else:
                logger.error(f"input_dir should be a filesystem path, not a URL: {input_dir}")
                return {"count": 0, "error": "Invalid input_dir"}

        if not output_dir:
            output_dir = Path.cwd() / "data" / "output" / "professionals"
        else:
            # Same check for output_dir
            if not output_dir.startswith(('http://', 'https://')):
                output_dir = Path(output_dir)
            else:
                logger.error(f"output_dir should be a filesystem path, not a URL: {output_dir}")
                return {"count": 0, "error": "Invalid output_dir"}

        output_html_dir = output_dir / "html"
        output_json_dir = output_dir / "json"
        output_html_dir.mkdir(parents=True, exist_ok=True)
        output_json_dir.mkdir(parents=True, exist_ok=True)

        # Validate URLs
        if not directory_url.startswith(('http://', 'https://')):
            logger.error(f"directory_url must be a valid HTTP(S) URL: {directory_url}")
            return {"count": 0, "error": "Invalid directory_url"}
        
        if not base_url.startswith(('http://', 'https://')):
            logger.error(f"base_url must be a valid HTTP(S) URL: {base_url}")
            return {"count": 0, "error": "Invalid base_url"}

        # Check input directory exists
        if not input_dir.exists():
            logger.error(f"Input directory does not exist: {input_dir}")
            return {
                "count": 0,
                "output_dir": str(output_dir),
                "error": f"Input directory not found: {input_dir}"
            }

        # Build hash set of already crawled carnets from existing files
        logger.info(f"Scanning output directory for existing files: {output_html_dir}")
        crawled_carnets = set()
        if output_html_dir.exists():
            for file in output_html_dir.glob("*-detail.html"):
                try:
                    # Extract carnet from filename (e.g., "12345-detail.html" -> "12345")
                    carnet = file.stem.replace("-detail", "")
                    crawled_carnets.add(carnet)
                except Exception as e:
                    logger.warning(f"Could not parse filename {file.name}: {e}")
        
            logger.info(f"Found {len(crawled_carnets)} already-crawled professionals in output directory")
    
        # Extract carnets from project HTML files
        logger.info(f"Extracting carnets from project HTML files in: {input_dir}")
        carnets_to_process = set()
        project_files = list(input_dir.glob("*.html"))
    
        logger.info(f"Found {len(project_files)} project HTML files to parse")
    
        for proj_file in project_files:
            try:
                with open(proj_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
            
                soup = BeautifulSoup(html_content, 'html.parser')
            
                # NEW APPROACH: Find the table row with "Carnet Profesional"
                # Look for <td> containing "Carnet Profesional" text
                carnet_cell = None
                for td in soup.find_all('td'):
                    if 'Carnet Profesional' in td.get_text(strip=True):
                        # Get the next <td> sibling which contains the value
                        carnet_cell = td.find_next_sibling('td')
                        break
                
                if carnet_cell:
                    # Extract text from <p> tag inside the cell
                    carnet_p = carnet_cell.find('p')
                    if carnet_p:
                        carnet_raw = carnet_p.get_text(strip=True)
                        if carnet_raw:
                            # Handle multiple carnets separated by comma
                            carnet = carnet_raw.split(',')[0].strip()
                            if carnet and carnet not in crawled_carnets:
                                carnets_to_process.add(carnet)
                                logger.debug(f"Extracted carnet {carnet} from {proj_file.name}")
                    else:
                        # Fallback: get text directly from td
                        carnet_raw = carnet_cell.get_text(strip=True)
                        if carnet_raw:
                            carnet = carnet_raw.split(',')[0].strip()
                            if carnet and carnet not in crawled_carnets:
                                carnets_to_process.add(carnet)
                                logger.debug(f"Extracted carnet {carnet} from {proj_file.name}")
        
            except Exception as e:
                logger.warning(f"Could not parse project file {proj_file.name}: {e}")
    
        logger.info(f"Extracted {len(carnets_to_process)} unique carnets to process")
        logger.info(f"Skipping {len(crawled_carnets)} already-crawled carnets")
    
        if not carnets_to_process:
            logger.info("No new carnets to process")
            return {
                "count": 0,
                "output_dir": str(output_dir),
                "total_carnets": 0,
                "skipped": len(crawled_carnets),
                "message": "No new carnets found"
            }
    
        # Limit to max_members if specified
        carnets_list = sorted(list(carnets_to_process))[:max_members]
        total_carnets = len(carnets_list)
    
        logger.info(f"Processing {total_carnets} carnets (limited to max_members={max_members})")
        logger.info("="*80)
    
        # Initialize HTTP headers
        self.headers["origin"] = base_url
        self.headers["referer"] = directory_url
    
        # Track results
        success_count = 0
        error_count = 0
    
        # Process each carnet
        for index, carnet in enumerate(carnets_list, start=1):
            logger.info(f"[{index}/{total_carnets}] Processing carnet: {carnet}")
        
            # Update progress
            if context:
                context.report_progress(
                    index,
                    total_carnets,
                    f"Processing carnet {carnet} ({index}/{total_carnets})",
                    {"success": success_count, "errors": error_count}
                )
        
            # Step 1: POST to members directory to get list
            payload = {
                "Consulta.CheckFiltro": "1",
                "Consulta.Dato": carnet,
                "Consulta.ColegioCiviles": "true",
                "Consulta.ColegioArquitectos": "true",
                "Consulta.ColegioCiemi": "true",
                "Consulta.ColegioTopografos": "true",
                "Consulta.ColegioTecnologos": "true",
                "Consulta.Provincia": "0",
                "Consulta.Canton": "0",
                "Consulta.Distrito": "0",
            }
        
            attempt = 0
            list_success = False
            html_list = None
        
            while attempt < max_retries and not list_success:
                attempt += 1
                try:
                    response = self.session.post(
                        directory_url,
                        headers=self.headers,
                        data=payload,
                        timeout=timeout
                    )
                
                    if response.status_code == 200:
                        html_list = response.text
                        # Save list HTML
                        list_file = output_html_dir / f"{carnet}.html"
                        with open(list_file, 'w', encoding='utf-8') as f:
                            f.write(html_list)
                        list_success = True
                        logger.debug(f"[{index}/{total_carnets}] Saved members list for {carnet}")
                    else:
                        logger.warning(f"[{index}/{total_carnets}] HTTP {response.status_code} for carnet {carnet} (attempt {attempt}/{max_retries})")
            
                except Exception as e:
                    logger.error(f"[{index}/{total_carnets}] Attempt {attempt}/{max_retries} failed for carnet {carnet}: {e}")
                    if attempt < max_retries:
                        backoff_time = 2 ** attempt
                        logger.info(f"[{index}/{total_carnets}] Waiting {backoff_time}s before retry...")
                        time.sleep(backoff_time)
        
            if not list_success or not html_list:
                error_count += 1
                logger.error(f"[{index}/{total_carnets}] ✗ Failed to get members list for carnet {carnet}")
                time.sleep(rate_limit)
                continue
        
            # Step 2: Extract detail URL from members list HTML
            # Pattern to find the detail link with matching carnet
            pat_exact = re.compile(
                rf"""
                    var\s*elemento\s*=\s*          # var elemento=
                    \\?["']                        #   quote optionally escaped
                    (?P<path>/ListadoMiembros/Miembros/DetalleMiembro\?cedula=\d+)
                    ["']\s*;?                      #   closing quote and optional ;
                    .*?                            #   anything (non-greedy)
                    <td\s+class=\\?['"]tablaMiembros\\?['"]>
                    \s*{re.escape(carnet)}\s*
                    </td>
                """,
                flags=re.IGNORECASE | re.DOTALL | re.VERBOSE
            )
        
            match = pat_exact.search(html_list)
        
            if not match:
                # Fallback: first elemento variable
                match = re.search(
                    r"var\s*elemento\s*=\s*\\?['\"](?P<path>/ListadoMiembros/Miembros/DetalleMiembro\?cedula=\d+)['\"]",
                    html_list,
                    flags=re.IGNORECASE
                )
                if match:
                    logger.debug(f"[{index}/{total_carnets}] Using fallback detail link for {carnet}")
        
            if not match:
                error_count += 1
                logger.error(f"[{index}/{total_carnets}] ✗ Could not find detail link for carnet {carnet}")
                time.sleep(rate_limit)
                continue
        
            detail_path = match.group("path").replace("\\/", "/")
            detail_url = base_url + detail_path
        
            # Step 3: GET detail page
            attempt = 0
            detail_success = False
        
            while attempt < max_retries and not detail_success:
                attempt += 1
                try:
                    response = self.session.get(
                        detail_url,
                        headers=self.headers,
                        timeout=timeout
                    )
                
                    if response.status_code == 200:
                        html_detail = response.text
                    
                        # Save detail HTML
                        detail_html_file = output_html_dir / f"{carnet}-detail.html"
                        with open(detail_html_file, 'w', encoding='utf-8') as f:
                            f.write(html_detail)
                    
                        # Parse and save JSON
                        soup = BeautifulSoup(html_detail, 'html.parser')
                        section = soup.select_one("section.container.documentsPage.seccionBuscador")
                        
                        if section:
                            inputs = section.find_all(['input', 'textarea'])
                            detail_json = {
                                t.get('name'): t.get('value', t.text.strip())
                                for t in inputs if t.get('name')
                            }
                            detail_json['carnet'] = carnet
                        
                            # Save JSON
                            detail_json_file = output_json_dir / f"{carnet}-detail.json"
                            with open(detail_json_file, 'w', encoding='utf-8') as f:
                                json.dump(detail_json, f, ensure_ascii=False, indent=2)
                        
                            logger.info(f"[{index}/{total_carnets}] ✓ Successfully crawled professional {carnet}")
                            success_count += 1
                            detail_success = True
                        else:
                            logger.warning(f"[{index}/{total_carnets}] No detail section found for {carnet}")
                            success_count += 1  # Still count as success - HTML saved
                            detail_success = True
                    else:
                        logger.warning(f"[{index}/{total_carnets}] HTTP {response.status_code} for detail {carnet} (attempt {attempt}/{max_retries})")
            
                except Exception as e:
                    logger.error(f"[{index}/{total_carnets}] Attempt {attempt}/{max_retries} failed for detail {carnet}: {e}")
                    if attempt < max_retries:
                        backoff_time = 2 ** attempt
                        logger.info(f"[{index}/{total_carnets}] Waiting {backoff_time}s before retry...")
                        time.sleep(backoff_time)
        
            if not detail_success:
                error_count += 1
                logger.error(f"[{index}/{total_carnets}] ✗ Failed to get detail for carnet {carnet}")
        
            # Rate limiting
            time.sleep(rate_limit)
    
        # Final summary
        logger.info("="*80)
        logger.info(f"Professionals crawl completed")
        logger.info(f"  Total carnets processed: {total_carnets}")
        logger.info(f"  ✓ Successfully crawled: {success_count}")
        logger.info(f"  ↷ Skipped (already crawled): {len(crawled_carnets)}")
        logger.info(f"  ✗ Errors: {error_count}")
        logger.info(f"  Output directory: {output_dir}")
        logger.info("="*80)

        return {
            "count": success_count,
            "output_dir": str(output_dir),
            "total_carnets": total_carnets,
            "errors": error_count,
            "skipped": len(crawled_carnets)
        }
