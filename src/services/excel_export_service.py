"""
Excel Export Service - Convert JSON data to Excel
"""
from typing import Dict, Any, Optional, List
import logging
import json
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class ExcelExportService:
    """Service for exporting JSON data to Excel format"""
    
    @staticmethod
    def normalize_keys(d: dict) -> dict:
        """Normalize dictionary keys for Excel export"""
        return {
            k.strip().lower().replace(" ", "_"): v
            for k, v in d.items()
            if k.lower() != "embedding"  # Skip embedding vectors
        }
    
    def export_json_to_excel(
        self,
        input_file: str,
        output_file: str,
        exclude_fields: Optional[List[str]] = None,
        flatten_nested: bool = False
    ) -> Dict[str, Any]:
        """
        Export JSON data to Excel file
        
        Args:
            input_file: Path to input JSON file (can be single object or array)
            output_file: Path to output Excel file
            exclude_fields: List of field names to exclude (e.g., 'embedding')
            flatten_nested: Whether to flatten nested objects
            
        Returns:
            Statistics about the export
        """
        logger.info("=" * 80)
        logger.info("Excel Export")
        logger.info("=" * 80)
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        
        if exclude_fields is None:
            exclude_fields = ["embedding", "embeddings", "vector"]
        
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Load JSON data
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle both single object and array of objects
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            records = data
        else:
            raise ValueError("Input JSON must be an object or array of objects")
        
        logger.info(f"  Loaded {len(records)} records")
        
        # Process records
        processed_records = []
        for record in records:
            if isinstance(record, dict):
                # Normalize keys and exclude fields
                processed = {
                    k: v for k, v in record.items()
                    if k.lower() not in [f.lower() for f in exclude_fields]
                }
                
                # Optional: flatten nested objects
                if flatten_nested:
                    processed = self._flatten_dict(processed)
                
                processed_records.append(processed)
        
        # Create DataFrame and export
        df = pd.DataFrame(processed_records)
        
        # Create output directory if needed
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export to Excel
        df.to_excel(output_file, index=False, engine='openpyxl')
        
        logger.info(f"  Exported {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"  Columns: {', '.join(df.columns[:10])}" + ("..." if len(df.columns) > 10 else ""))
        logger.info("✅ Excel export completed")
        
        return {
            'status': 'success',
            'input_file': input_file,
            'output_file': output_file,
            'records_exported': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns)
        }
    
    def _flatten_dict(self, d: dict, parent_key: str = '', sep: str = '_') -> dict:
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                # Convert list of dicts to JSON string
                items.append((new_key, json.dumps(v, ensure_ascii=False)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def export_directory_to_excel(
        self,
        input_dir: str,
        output_file: str,
        pattern: str = "*.json",
        exclude_fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Export multiple JSON files from a directory to a single Excel file
        
        Args:
            input_dir: Path to directory containing JSON files
            output_file: Path to output Excel file
            pattern: Glob pattern for JSON files (default: *.json)
            exclude_fields: List of field names to exclude
            
        Returns:
            Statistics about the export
        """
        logger.info("=" * 80)
        logger.info("Excel Export from Directory")
        logger.info("=" * 80)
        logger.info(f"  Input Dir: {input_dir}")
        logger.info(f"  Pattern: {pattern}")
        logger.info(f"  Output: {output_file}")
        
        if exclude_fields is None:
            exclude_fields = ["embedding", "embeddings", "vector"]
        
        input_path = Path(input_dir)
        if not input_path.exists():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")
        
        # Find all JSON files
        files = sorted(input_path.glob(pattern))
        logger.info(f"  Found {len(files)} files")
        
        # Load all records
        all_records = []
        errors = 0
        
        for file_path in files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                if isinstance(data, dict):
                    # Single object
                    processed = {
                        k: v for k, v in data.items()
                        if k.lower() not in [f.lower() for f in exclude_fields]
                    }
                    all_records.append(processed)
                elif isinstance(data, list):
                    # Array of objects
                    for record in data:
                        if isinstance(record, dict):
                            processed = {
                                k: v for k, v in record.items()
                                if k.lower() not in [f.lower() for f in exclude_fields]
                            }
                            all_records.append(processed)
            except Exception as e:
                logger.warning(f"  Failed to process {file_path.name}: {e}")
                errors += 1
        
        logger.info(f"  Loaded {len(all_records)} records ({errors} errors)")
        
        # Create DataFrame and export
        df = pd.DataFrame(all_records)
        
        # Create output directory if needed
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export to Excel
        df.to_excel(output_file, index=False, engine='openpyxl')
        
        logger.info(f"  Exported {len(df)} rows, {len(df.columns)} columns")
        logger.info("✅ Excel export completed")
        
        return {
            'status': 'success',
            'input_dir': input_dir,
            'output_file': output_file,
            'files_processed': len(files),
            'records_exported': len(df),
            'columns': len(df.columns),
            'errors': errors
        }
