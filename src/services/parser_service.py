"""
Parser Service for HTML to JSON conversion
"""
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class ParserService:
    """Service for parsing HTML files to JSON"""
    
    def __init__(self):
        """Initialize parser service"""
        pass
    
    def parse_html_batch(
        self,
        input_dir: str,
        output_dir: str,
        save_json: bool = True,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Parse batch of HTML files to JSON format
        
        Args:
            input_dir: Directory containing HTML files
            output_dir: Directory to save JSON files
            save_json: Whether to save output as JSON files (default: True)
            context: Optional context for progress reporting
            
        Returns:
            Dictionary with parse results
        """
        logger.info(f"Starting batch HTML parsing: {input_dir} -> {output_dir}")
        logger.info(f"Settings - save_json: {save_json}")
        
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        if save_json:
            output_path.mkdir(parents=True, exist_ok=True)
        
        # Check input directory exists
        if not input_path.exists():
            logger.error(f"Input directory does not exist: {input_dir}")
            return {
                "count": 0,
                "output_dir": str(output_dir),
                "error": f"Input directory not found: {input_dir}"
            }
        
        # Import parser here to avoid circular imports
        try:
            from etl.extract.html_parser import parse_project_html_file
            logger.info("Using html_parser.parse_project_html_file")
        except ImportError:
            logger.warning("Could not import html_parser, trying html_to_json")
            try:
                from etl.extract.html_to_json import process_file
                parse_project_html_file = lambda filepath: process_file(filepath)
                logger.info("Using html_to_json.process_file")
            except ImportError:
                logger.error("Could not import HTML parser module")
                return {
                    "count": 0,
                    "output_dir": str(output_dir),
                    "error": "HTML parser module not found"
                }
        
        # Get HTML files
        html_files = list(input_path.glob("*.html"))
        total_files = len(html_files)
        
        if total_files == 0:
            logger.warning(f"No HTML files found in {input_dir}")
            return {
                "count": 0,
                "output_dir": str(output_dir),
                "message": "No HTML files to parse"
            }
        
        logger.info(f"Found {total_files} HTML files to parse")
        
        # Track results
        success_count = 0
        error_count = 0
        parsed_data_list = []
        
        # Parse each file
        for index, html_file in enumerate(html_files, start=1):
            logger.info(f"[{index}/{total_files}] Parsing {html_file.name}")
            
            # Update progress
            if context and hasattr(context, 'report_progress'):
                context.report_progress(
                    index,
                    total_files,
                    f"Parsing {html_file.name} ({index}/{total_files})",
                    {"success": success_count, "errors": error_count}
                )
            
            try:
                # Parse HTML file
                parsed_data = parse_project_html_file(str(html_file))
                
                if parsed_data:
                    parsed_data_list.append(parsed_data)
                    
                    if save_json:
                        json_file = output_path / f"{html_file.stem}.json"
                        with open(json_file, 'w', encoding='utf-8') as f:
                            json.dump(parsed_data, f, ensure_ascii=False, indent=2)
                        logger.info(f"[{index}/{total_files}] ✓ Saved {json_file.name}")
                    else:
                        logger.info(f"[{index}/{total_files}] ✓ Parsed {html_file.name}")
                    
                    success_count += 1
                else:
                    logger.warning(f"[{index}/{total_files}] No data extracted from {html_file.name}")
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"[{index}/{total_files}] ✗ Failed to parse {html_file.name}: {e}")
                error_count += 1
        
        # Final summary
        logger.info("="*80)
        logger.info(f"HTML parsing completed")
        logger.info(f"  Total files: {total_files}")
        logger.info(f"  ✓ Successfully parsed: {success_count}")
        logger.info(f"  ✗ Errors: {error_count}")
        if save_json:
            logger.info(f"  Output directory: {output_dir}")
        logger.info("="*80)
        
        result = {
            "count": success_count,
            "output_dir": str(output_dir),
            "total_files": total_files,
            "errors": error_count
        }
        
        if not save_json:
            result["data"] = parsed_data_list
        
        return result
    
    def parse_html_to_json(
        self,
        input_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        batch_mode: bool = True,
        save_json: bool = True,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Parse HTML files to JSON format (alternative interface)
        
        Args:
            input_dir: Directory containing HTML files
            output_dir: Directory to save JSON files
            batch_mode: Process all files in directory
            save_json: Save output as JSON files
            context: Optional context for progress reporting
            
        Returns:
            Dictionary with parse results
        """
        if not input_dir:
            input_dir = str(Path.cwd() / "data" / "output" / "html")
        
        if not output_dir:
            output_dir = str(Path.cwd() / "data" / "output" / "json")
        
        # Delegate to parse_html_batch
        return self.parse_html_batch(input_dir, output_dir, save_json, context)