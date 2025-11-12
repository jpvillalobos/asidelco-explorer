from typing import Dict, List, Optional

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

try:
    from etl.extract.professional_crawler import ProfessionalCrawler
except ImportError:
    # Fallback when services is imported as a subpackage
    from ..etl.extract.professional_crawler import ProfessionalCrawler
from ..etl.extract.project_crawler import ProjectCrawler


class CrawlerService:
    """Service for HTML parsing operations"""
    
    def __init__(self):
        """Initialize parser service"""
        self.parser = ProfessionalCrawler()
    
    def parse_html(
        self, 
        html_content: str,
        parser_type: str = 'lxml'
    ) -> Dict:
        """
        Parse HTML content
        
        Args:
            html_content: Raw HTML string
            parser_type: Parser type (lxml, html.parser, etc.)
            
        Returns:
            Parsed data dictionary
        """
        return self.parser.parse(html_content, parser_type=parser_type)
    
    def parse_html_file(
        self, 
        file_path: Union[str, Path],
        encoding: str = 'utf-8'
    ) -> Dict:
        """
        Parse HTML from file
        
        Args:
            file_path: Path to HTML file
            encoding: File encoding
            
        Returns:
            Parsed data dictionary
        """
        file_path = Path(file_path)
        with open(file_path, 'r', encoding=encoding) as f:
            html_content = f.read()
        return self.parse_html(html_content)
    
    def html_to_json(
        self, 
        html_content: str,
        output_path: Optional[Union[str, Path]] = None
    ) -> Dict:
        """
        Convert HTML to JSON format
        
        Args:
            html_content: Raw HTML string
            output_path: Optional path to save JSON output
            
        Returns:
            JSON data dictionary
        """
        json_data = html_to_json(html_content)
        
        if output_path:
            import json
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        return json_data
    
    def batch_parse_files(
        self, 
        file_paths: List[Union[str, Path]],
        encoding: str = 'utf-8'
    ) -> List[Dict]:
        """
        Parse multiple HTML files
        
        Args:
            file_paths: List of file paths
            encoding: File encoding
            
        Returns:
            List of parsed data dictionaries
        """
        results = []
        for file_path in file_paths:
            try:
                result = self.parse_html_file(file_path, encoding=encoding)
                results.append(result)
            except Exception as e:
                results.append({'error': str(e), 'file': str(file_path)})
        return results


"""
Crawler Service with Progress Reporting
"""
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CrawlerService:
    """Service for web crawling with progress reporting"""
    
    def crawl_professionals(
        self,
        output_dir: str,
        context: Optional[object] = None,
        max_pages: int = 100
    ) -> Dict[str, Any]:
        """
        Crawl professional profiles with progress reporting
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        results = []
        errors = []
        
        # Simulate crawling with progress reporting
        for page_num in range(1, min(max_pages + 1, 11)):  # Limit to 10 for demo
            try:
                if context:
                    context.report_progress(
                        page_num,
                        max_pages,
                        f"Crawling page {page_num}/{max_pages}",
                        {
                            "page": page_num,
                            "results_so_far": len(results),
                            "errors": len(errors)
                        }
                    )
                
                # Simulate crawling result
                page_result = {
                    "page": page_num,
                    "url": f"https://example.com/professionals?page={page_num}",
                    "professionals": [
                        {"name": f"Professional {page_num}-{i}", "id": f"{page_num}_{i}"}
                        for i in range(1, 6)
                    ]
                }
                results.append(page_result)
                
                # Simulate some processing time
                import time
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error crawling page {page_num}: {e}")
                errors.append({"page": page_num, "error": str(e)})
        
        # Save results
        import json
        results_file = output_path / "professionals.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        summary = {
            "total_results": len(results),
            "total_errors": len(errors),
            "output_dir": str(output_path),
            "results_file": str(results_file)
        }
        
        if context:
            context.report_progress(
                max_pages,
                max_pages,
                f"Crawling complete: {len(results)} results",
                summary
            )
        
        logger.info(f"Crawling completed: {len(results)} results, {len(errors)} errors")
        return summary