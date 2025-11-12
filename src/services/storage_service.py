"""
Storage Service
"""
from typing import Any, Dict, List, Optional, Union
import logging
from pathlib import Path
import json
import pandas as pd
import sys

sys.path.append(str(Path(__file__).parent.parent))

from .csv_service import CSVService


class StorageService:
    """Service for file storage operations"""
    
    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        """
        Initialize storage service
        
        Args:
            base_path: Base directory for file operations
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.csv_service = CSVService()
    
    def save_json(
        self,
        data: Union[Dict, List],
        filename: str,
        subdirectory: Optional[str] = None,
        indent: int = 2
    ) -> str:
        """Save data as JSON file"""
        output_dir = self.base_path / subdirectory if subdirectory else self.base_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        
        return str(output_path)
    
    def load_json(
        self,
        filename: str,
        subdirectory: Optional[str] = None
    ) -> Union[Dict, List]:
        """Load data from JSON file"""
        input_dir = self.base_path / subdirectory if subdirectory else self.base_path
        input_path = input_dir / filename
        
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def save_csv(
        self,
        data: Union[pd.DataFrame, List[Dict]],
        filename: str,
        subdirectory: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Save data as CSV file
        
        Args:
            data: DataFrame or list of dictionaries
            filename: Output filename
            subdirectory: Optional subdirectory
            **kwargs: Additional CSV write arguments
            
        Returns:
            Path to saved file
        """
        output_dir = self.base_path / subdirectory if subdirectory else self.base_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / filename
        
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
        
        return self.csv_service.write_csv(df, output_path, **kwargs)
    
    def load_csv(
        self,
        filename: str,
        subdirectory: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load data from CSV file
        
        Args:
            filename: Input filename
            subdirectory: Optional subdirectory
            **kwargs: Additional CSV read arguments
            
        Returns:
            DataFrame with loaded data
        """
        input_dir = self.base_path / subdirectory if subdirectory else self.base_path
        input_path = input_dir / filename
        
        return self.csv_service.read_csv(input_path, **kwargs)
    
    def csv_to_json(
        self,
        csv_filename: str,
        json_filename: str,
        subdirectory: Optional[str] = None,
        orient: str = 'records'
    ) -> str:
        """
        Convert CSV to JSON
        
        Args:
            csv_filename: Input CSV filename
            json_filename: Output JSON filename
            subdirectory: Optional subdirectory
            orient: JSON orientation ('records', 'index', 'columns', etc.)
            
        Returns:
            Path to JSON file
        """
        df = self.load_csv(csv_filename, subdirectory=subdirectory)
        data = df.to_dict(orient=orient)
        return self.save_json(data, json_filename, subdirectory=subdirectory)
    
    def json_to_csv(
        self,
        json_filename: str,
        csv_filename: str,
        subdirectory: Optional[str] = None
    ) -> str:
        """
        Convert JSON to CSV
        
        Args:
            json_filename: Input JSON filename
            csv_filename: Output CSV filename
            subdirectory: Optional subdirectory
            
        Returns:
            Path to CSV file
        """
        data = self.load_json(json_filename, subdirectory=subdirectory)
        return self.save_csv(data, csv_filename, subdirectory=subdirectory)
    
    def list_files(
        self,
        subdirectory: Optional[str] = None,
        pattern: str = "*"
    ) -> List[str]:
        """List files in directory"""
        search_dir = self.base_path / subdirectory if subdirectory else self.base_path
        return [str(p) for p in search_dir.glob(pattern)]