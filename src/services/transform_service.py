"""
Transform Service
"""
from typing import List, Dict, Any, Optional, Union
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class TransformService:
    """Service for data transformation operations"""
    
    def enrich_data(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        context: Optional[object] = None
    ) -> List[Dict[str, Any]]:
        """
        Enrich data with additional information
        """
        # Convert DataFrame to list of dicts if needed
        if isinstance(data, pd.DataFrame):
            data_list = data.to_dict('records')
        else:
            data_list = data
        
        if context:
            context.report_progress(0, len(data_list), "Starting data enrichment")
        
        enriched_data = []
        
        for i, record in enumerate(data_list):
            try:
                # Your enrichment logic here
                enriched_record = record.copy() if isinstance(record, dict) else record
                
                # Add enrichment fields
                enriched_record['enriched_timestamp'] = pd.Timestamp.now().isoformat()
                enriched_record['record_id'] = f"rec_{i:06d}"
                enriched_record['processing_status'] = 'enriched'
                
                # Add more enrichment logic as needed
                if 'name' in enriched_record and enriched_record['name']:
                    enriched_record['name_length'] = len(str(enriched_record['name']))
                
                enriched_data.append(enriched_record)
                
                if context:
                    context.report_progress(
                        i + 1,
                        len(data_list),
                        f"Enriched record {i + 1}/{len(data_list)}",
                        {"record_id": enriched_record.get('record_id', 'unknown')}
                    )
                    
            except Exception as e:
                logger.error(f"Error enriching record {i}: {e}")
                enriched_data.append(record)  # Keep original if enrichment fails
        
        logger.info(f"Enriched {len(enriched_data)} records")
        return enriched_data
    
    def merge_excel(
        self,
        files: List[str],
        output_file: str,
        context: Optional[object] = None
    ) -> str:
        """
        Merge multiple Excel files
        """
        if context:
            context.report_progress(0, len(files), "Starting Excel merge")
        
        dfs = []
        
        for i, file_path in enumerate(files):
            try:
                if Path(file_path).suffix.lower() == '.csv':
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                
                # Add source file column
                df['source_file'] = Path(file_path).name
                dfs.append(df)
                
                if context:
                    context.report_progress(
                        i + 1,
                        len(files),
                        f"Loaded {i + 1}/{len(files)} files",
                        {"current_file": Path(file_path).name, "rows": len(df)}
                    )
                    
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
        
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            
            # Ensure output directory exists
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            merged_df.to_excel(output_file, index=False)
            
            if context:
                context.report_progress(
                    len(files),
                    len(files),
                    f"Merged {len(dfs)} files into {output_file}",
                    {"total_rows": len(merged_df), "output_file": output_file}
                )
            
            logger.info(f"Merged {len(dfs)} files into {output_file}")
            return output_file
        
        raise ValueError("No files to merge")