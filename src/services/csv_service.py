"""
CSV Service with Progress Reporting
"""
import pandas as pd
from pathlib import Path
from typing import Optional, Union, Dict, Any, List, Callable
from io import BytesIO
import logging
import unicodedata

logger = logging.getLogger(__name__)


class CSVService:
    """Service for CSV operations with progress reporting"""
    
    def read_file(
        self,
        file_path: Union[str, Path],
        context: Optional[object] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read Excel or CSV file with automatic conversion
        
        Args:
            file_path: Path to file (xlsx, xls, or csv)
            context: Optional context for progress reporting
            **kwargs: Additional arguments for pandas read functions
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        if context:
            context.report_progress(0, 100, f"Opening file: {path.name}")
        
        logger.info(f"Reading file: {path}")
        
        # Check file extension
        suffix = path.suffix.lower()
        
        if suffix in ['.xlsx', '.xls']:
            # Read Excel file
            logger.info(f"Reading Excel file: {path}")
            df = pd.read_excel(file_path, **kwargs)
            
            # Save CSV version to same directory
            csv_path = path.with_suffix('.csv')
            logger.info(f"Converting to CSV: {csv_path}")
            df.to_csv(csv_path, index=False)
            logger.info(f"CSV saved: {csv_path}")
            
        elif suffix == '.csv':
            # Read CSV file
            logger.info(f"Reading CSV file: {path}")
            df = pd.read_csv(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {suffix}. Use .xlsx, .xls, or .csv")
        
        if context:
            context.report_progress(
                100,
                100,
                f"Loaded {len(df)} rows",
                {"rows": len(df), "columns": len(df.columns)}
            )
        
        logger.info(f"Loaded data: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def read_csv(
        self,
        file_path: Union[str, Path],
        context: Optional[object] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read CSV file with progress reporting
        Automatically converts Excel to CSV if needed
        """
        return self.read_file(file_path, context, **kwargs)
    
    def excel_to_csv(
        self,
        excel_path: Union[str, Path],
        csv_path: Optional[Union[str, Path]] = None,
        context: Optional[object] = None
    ) -> Path:
        """
        Convert Excel file to CSV
        
        Args:
            excel_path: Path to Excel file
            csv_path: Optional output CSV path (auto-generated if None)
            context: Optional context for progress reporting
            
        Returns:
            Path to created CSV file
        """
        excel_path = Path(excel_path)
        
        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
        
        if context:
            context.report_progress(0, 100, f"Reading Excel: {excel_path.name}")
        
        logger.info(f"Converting Excel to CSV: {excel_path}")
        
        # Read Excel
        df = pd.read_excel(excel_path)
        
        # Determine CSV path
        if csv_path is None:
            csv_path = excel_path.with_suffix('.csv')
        else:
            csv_path = Path(csv_path)
        
        # Ensure output directory exists
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        if context:
            context.report_progress(50, 100, f"Writing CSV: {csv_path.name}")
        
        # Write CSV
        df.to_csv(csv_path, index=False)
        
        if context:
            context.report_progress(
                100,
                100,
                f"Converted: {len(df)} rows",
                {"rows": len(df), "columns": len(df.columns), "csv_path": str(csv_path)}
            )
        
        logger.info(f"CSV saved: {csv_path} ({len(df)} rows)")
        return csv_path
    
    def _normalize_text(self, text):
        """Normalize text: uppercase and remove accents"""
        if pd.isna(text) or text == '':
            return text
        
        # Convert to string and uppercase
        text = str(text).upper()
        
        # Remove accents using Unicode normalization
        normalized = unicodedata.normalize('NFD', text)
        ascii_text = normalized.encode('ascii', 'ignore').decode('ascii')
        
        return ascii_text
    
    def _generate_unique_ids(self, df: pd.DataFrame, project_column: str = 'proyecto') -> pd.DataFrame:
        """Generate unique IDs based on project column + sequence number"""
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Group by project and add sequence numbers
        df_copy['_seq'] = df_copy.groupby(project_column).cumcount() + 1
        
        # Create unique ID as project-sequence
        df_copy['id'] = df_copy[project_column].astype(str) + '-' + df_copy['_seq'].astype(str)
        
        # Drop the temporary sequence column
        df_copy = df_copy.drop('_seq', axis=1)
        
        # Reorder columns to put 'id' first
        columns = ['id'] + [col for col in df_copy.columns if col != 'id']
        df_copy = df_copy[columns]
        
        return df_copy
    
    def normalize_csv(
        self,
        input_file: Union[str, Path],
        output_file: Union[str, Path],
        context: Optional[object] = None
    ) -> pd.DataFrame:
        """Normalize CSV data with progress reporting"""
        if context:
            context.report_progress(0, 100, "Reading input file")
        
        # Use read_file to support both Excel and CSV
        df = self.read_file(input_file)
        
        # Generate unique IDs
        if context:
            context.report_progress(15, 100, "Generating unique IDs")
        
        # Check if 'proyecto' column exists, otherwise use first column
        project_col = 'proyecto' if 'proyecto' in df.columns else df.columns[0]
        df = self._generate_unique_ids(df, project_col)
        logger.info(f"Generated unique IDs using column: {project_col}")
        
        # Clean column names
        if context:
            context.report_progress(30, 100, "Cleaning column names")
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False)
        
        # Normalize text data (uppercase and remove accents)
        if context:
            context.report_progress(45, 100, "Normalizing text data")
        
        for col in df.columns:
            if df[col].dtype == 'object':  # Text columns
                df[col] = df[col].apply(self._normalize_text)
        
        logger.info("Applied text normalization (uppercase, no accents)")
        
        # Remove duplicates
        if context:
            context.report_progress(60, 100, "Removing duplicates")
        original_count = len(df)
        df = df.drop_duplicates()
        removed_duplicates = original_count - len(df)
        
        if removed_duplicates > 0:
            logger.info(f"Removed {removed_duplicates} duplicate rows")
        
        # Handle missing values
        if context:
            context.report_progress(75, 100, "Handling missing values")
        df = df.fillna('')
        
        # Save output
        if context:
            context.report_progress(90, 100, f"Saving to {output_file}")
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        
        if context:
            context.report_progress(
                100,
                100,
                f"Normalization complete: {len(df)} rows",
                {"output_file": str(output_file), "rows": len(df), "duplicates_removed": removed_duplicates}
            )
        
        logger.info(f"Normalized CSV saved to: {output_file}")
        return df
    
    def write_csv(
        self,
        data: pd.DataFrame,
        file_path: Union[str, Path],
        context: Optional[object] = None,
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> str:
        """Write DataFrame to CSV file"""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if context:
            context.report_progress(0, 100, f"Writing to {path.name}")
        
        if columns:
            data = data[columns]
        
        data.to_csv(path, index=False, **kwargs)
        
        if context:
            context.report_progress(
                100,
                100,
                f"Saved {len(data)} rows to {path.name}",
                {"rows": len(data), "file_size": path.stat().st_size}
            )
        
        logger.info(f"CSV saved to: {path}")
        return str(path)