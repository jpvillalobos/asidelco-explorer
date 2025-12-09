"""
Merge Service - Combines CSV, Project JSON, and Professional JSON data
"""
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import logging
import json
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class MergeService:
    """Service for merging CSV, Project, and Professional data"""
    
    def __init__(self):
        """Initialize merge service"""
        self.stats = {
            "csv_rows_processed": 0,
            "projects_matched": 0,
            "projects_missing": 0,
            "professionals_matched": 0,
            "professionals_missing": 0,
            "output_records": 0
        }
    
    def merge_data_sources(
        self,
        csv_file: str,
        projects_json_dir: str,
        professionals_json_dir: str,
        output_file: str,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Merge CSV, Project JSON, and Professional JSON data
        
        Strategy:
        - Each CSV row (id: proyecto-sequence) is ONE output record
        - CSV row links to Project JSON via 'proyecto' field
        - Project JSON links to Professional JSON via 'Carnet Profesional'
        - Multiple CSV rows can share same project (different buildings/areas)
        
        Args:
            csv_file: Path to normalized CSV file
            projects_json_dir: Directory with project JSON files
            professionals_json_dir: Directory with professional JSON files
            output_file: Path to output merged JSON file
            context: Optional context for progress reporting
            
        Returns:
            Dictionary with merge results
        """
        logger.info("="*80)
        logger.info("Starting data merge process")
        logger.info(f"  CSV: {csv_file}")
        logger.info(f"  Projects: {projects_json_dir}")
        logger.info(f"  Professionals: {professionals_json_dir}")
        logger.info(f"  Output: {output_file}")
        logger.info("="*80)
        
        # Load CSV
        if context:
            context.report_progress(0, 100, "Loading CSV file")
        
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"✓ Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
        
        # Load project JSONs into lookup dict
        if context:
            context.report_progress(10, 100, "Loading project JSON files")
        
        projects_lookup = self._load_json_files(projects_json_dir, "project_id")
        logger.info(f"✓ Loaded {len(projects_lookup)} project JSON files")
        
        # Load professional JSONs into lookup dict
        if context:
            context.report_progress(20, 100, "Loading professional JSON files")
        
        professionals_lookup = self._load_json_files(professionals_json_dir, "Carne")
        logger.info(f"✓ Loaded {len(professionals_lookup)} professional JSON files")
        
        # Process each CSV row
        if context:
            context.report_progress(30, 100, "Merging data sources")
        
        merged_records = []
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            self.stats["csv_rows_processed"] += 1
            
            # Progress reporting
            if context and idx % 100 == 0:
                progress = 30 + int((idx / total_rows) * 60)
                context.report_progress(
                    progress,
                    100,
                    f"Processing row {idx + 1}/{total_rows}",
                    {
                        "csv_rows": self.stats["csv_rows_processed"],
                        "projects_matched": self.stats["projects_matched"],
                        "professionals_matched": self.stats["professionals_matched"]
                    }
                )
            
            # Create merged record
            merged_record = self._merge_single_row(
                row,
                projects_lookup,
                professionals_lookup,
                idx
            )
            
            merged_records.append(merged_record)
        
        # Save merged data
        if context:
            context.report_progress(90, 100, f"Saving {len(merged_records)} merged records")
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(merged_records, f, ensure_ascii=False, indent=2)
        
        self.stats["output_records"] = len(merged_records)
        
        logger.info("="*80)
        logger.info("Merge completed successfully")
        logger.info(f"  CSV rows processed: {self.stats['csv_rows_processed']}")
        logger.info(f"  Projects matched: {self.stats['projects_matched']}")
        logger.info(f"  Projects missing: {self.stats['projects_missing']}")
        logger.info(f"  Professionals matched: {self.stats['professionals_matched']}")
        logger.info(f"  Professionals missing: {self.stats['professionals_missing']}")
        logger.info(f"  Output records: {self.stats['output_records']}")
        logger.info(f"  Output file: {output_path}")
        logger.info("="*80)
        
        if context:
            context.report_progress(
                100,
                100,
                f"Merge complete: {len(merged_records)} records",
                self.stats
            )
        
        return {
            "count": len(merged_records),
            "output_file": str(output_path),
            "stats": self.stats.copy()
        }
    
    def _load_json_files(
        self,
        directory: str,
        key_field: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Load all JSON files from directory into lookup dictionary
        
        Args:
            directory: Directory containing JSON files
            key_field: Field to use as lookup key
            
        Returns:
            Dictionary mapping key values to JSON data
        """
        lookup = {}
        dir_path = Path(directory)
        
        if not dir_path.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return lookup
        
        json_files = list(dir_path.glob("*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Get key value
                key = data.get(key_field)
                
                if key:
                    # Handle multiple carnets (comma-separated)
                    if key_field == "Carne" and "," in str(key):
                        # Store under first carnet
                        key = str(key).split(",")[0].strip()
                    
                    lookup[str(key).strip()] = data
                else:
                    logger.debug(f"No '{key_field}' in {json_file.name}")
                    
            except Exception as e:
                logger.warning(f"Failed to load {json_file.name}: {e}")
        
        return lookup
    
    def _merge_single_row(
        self,
        csv_row: pd.Series,
        projects_lookup: Dict[str, Dict],
        professionals_lookup: Dict[str, Dict],
        row_index: int
    ) -> Dict[str, Any]:
        """
        Merge a single CSV row with project and professional data
        
        Creates a structured record with:
        - csv_data: Original CSV fields
        - project_data: Fields from project JSON
        - professional_data: Fields from professional JSON
        - metadata: Merge metadata (timestamps, warnings, etc.)
        
        Args:
            csv_row: Pandas Series representing CSV row
            projects_lookup: Project JSON lookup dict
            professionals_lookup: Professional JSON lookup dict
            row_index: Row index for logging
            
        Returns:
            Merged record dictionary
        """
        # Initialize merged record
        merged_record = {
            "record_id": csv_row.get("id", f"row_{row_index}"),
            "csv_data": {},
            "project_data": {},
            "professional_data": {},
            "metadata": {
                "merged_at": datetime.now().isoformat(),
                "row_index": row_index,
                "warnings": []
            }
        }
        
        # Add CSV data (convert to dict, handle NaN)
        csv_dict = csv_row.to_dict()
        merged_record["csv_data"] = {
            k: (None if pd.isna(v) else v) 
            for k, v in csv_dict.items()
        }
        
        # Get proyecto number for lookup
        proyecto = str(csv_row.get("proyecto", "")).strip()
        
        if not proyecto:
            merged_record["metadata"]["warnings"].append("Missing proyecto number in CSV")
            logger.warning(f"Row {row_index}: Missing proyecto number")
            self.stats["projects_missing"] += 1
            return merged_record
        
        # Look up project JSON
        project_json = projects_lookup.get(proyecto)
        
        if project_json:
            merged_record["project_data"] = project_json.copy()
            self.stats["projects_matched"] += 1
            logger.debug(f"Row {row_index}: Matched project {proyecto}")
            
            # Look up professional via carnet
            carnet = project_json.get("Carnet Profesional", "").strip()
            
            if carnet:
                # Handle multiple carnets (comma-separated)
                if "," in carnet:
                    carnet = carnet.split(",")[0].strip()
                    merged_record["metadata"]["warnings"].append(
                        f"Multiple carnets found, using first: {carnet}"
                    )
                
                professional_json = professionals_lookup.get(carnet)
                
                if professional_json:
                    merged_record["professional_data"] = professional_json.copy()
                    self.stats["professionals_matched"] += 1
                    logger.debug(f"Row {row_index}: Matched professional {carnet}")
                else:
                    merged_record["metadata"]["warnings"].append(
                        f"Professional not found for carnet: {carnet}"
                    )
                    logger.warning(f"Row {row_index}: Professional not found for carnet {carnet}")
                    self.stats["professionals_missing"] += 1
            else:
                merged_record["metadata"]["warnings"].append(
                    "No Carnet Profesional in project data"
                )
                logger.warning(f"Row {row_index}: No carnet in project {proyecto}")
                self.stats["professionals_missing"] += 1
        else:
            merged_record["metadata"]["warnings"].append(
                f"Project not found: {proyecto}"
            )
            logger.warning(f"Row {row_index}: Project not found: {proyecto}")
            self.stats["projects_missing"] += 1
        
        return merged_record
    
    def get_stats(self) -> Dict[str, Any]:
        """Get merge statistics"""
        return self.stats.copy()