"""
Transform Service
"""
from typing import List, Dict, Any, Optional, Union
import logging
import pandas as pd
from pathlib import Path
import json
import re
from unidecode import unidecode
from dateutil import parser as date_parser
from datetime import datetime

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

    def flatten_normalize(
        self,
        input_file: str,
        output_file: str,
        normalize_text: bool = True,
        normalize_dates: bool = True,
        uppercase_fields: Optional[List[str]] = None,
        titlecase_fields: Optional[List[str]] = None,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Flatten nested JSON structure and normalize data.

        Args:
            input_file: Path to merged JSON file with nested structure
            output_file: Path to output flattened/normalized JSON file
            normalize_text: Whether to uppercase and remove accents from text fields
            normalize_dates: Whether to normalize date fields to ISO format
            uppercase_fields: List of field patterns to uppercase (default: name, address fields)
            titlecase_fields: List of field patterns to titlecase (default: description fields)
            context: Optional context for progress reporting

        Returns:
            Dict with status, count, and stats
        """
        logger.info(f"Starting flatten and normalize operation")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Normalize text: {normalize_text}")
        logger.info(f"  Normalize dates: {normalize_dates}")

        # Default field patterns
        if uppercase_fields is None:
            uppercase_fields = ['name', 'nombre', 'apellido', 'address', 'direccion', 'province', 'provincia', 'canton', 'district', 'distrito']
        if titlecase_fields is None:
            titlecase_fields = ['description', 'descripcion', 'notes', 'notas', 'observaciones']

        # Read input JSON
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        logger.info(f"Loading merged data from {input_file}")
        with open(input_file, 'r', encoding='utf-8') as f:
            merged_data = json.load(f)

        total_records = len(merged_data)
        logger.info(f"Loaded {total_records} records")

        if total_records == 0:
            logger.warning("No records found in input file!")
            return {
                'status': 'warning',
                'message': 'No records to process',
                'output_file': output_file,
                'count': 0,
                'stats': {}
            }

        if context:
            context.report_progress(0, total_records, "Starting data flattening and normalization")

        flattened_data = []
        stats = {
            'total_records': total_records,
            'processed': 0,
            'errors': 0,
            'text_normalized_count': 0,
            'dates_normalized_count': 0,
            'numeric_fields_cleaned': 0,
            'field_names_sanitized': 0
        }

        for i, record in enumerate(merged_data):
            try:
                # Create flattened record
                flat_record = {}

                # Add record_id first
                flat_record['record_id'] = record.get('record_id', f'rec_{i}')

                # Flatten csv_data fields with prefix
                if 'csv_data' in record and record['csv_data']:
                    for key, value in record['csv_data'].items():
                        sanitized_key = self._sanitize_field_name(f'csv_{key}')
                        flat_record[sanitized_key] = value
                        if sanitized_key != f'csv_{key}':
                            stats['field_names_sanitized'] += 1

                # Flatten project_data fields with prefix
                if 'project_data' in record and record['project_data']:
                    for key, value in record['project_data'].items():
                        sanitized_key = self._sanitize_field_name(f'project_{key}')
                        flat_record[sanitized_key] = value
                        if sanitized_key != f'project_{key}':
                            stats['field_names_sanitized'] += 1

                # Flatten professional_data fields with prefix
                if 'professional_data' in record and record['professional_data']:
                    for key, value in record['professional_data'].items():
                        sanitized_key = self._sanitize_field_name(f'professional_{key}')
                        flat_record[sanitized_key] = value
                        if sanitized_key != f'professional_{key}':
                            stats['field_names_sanitized'] += 1

                # Add metadata fields
                if 'metadata' in record and record['metadata']:
                    for key, value in record['metadata'].items():
                        sanitized_key = self._sanitize_field_name(f'metadata_{key}')
                        flat_record[sanitized_key] = value
                        if sanitized_key != f'metadata_{key}':
                            stats['field_names_sanitized'] += 1

                # Clean numeric fields (remove trailing decimals)
                numeric_cleaned = self._clean_numeric_fields(flat_record)
                stats['numeric_fields_cleaned'] += numeric_cleaned

                # Normalize text fields
                if normalize_text:
                    text_normalized = self._normalize_text_fields(flat_record, uppercase_fields, titlecase_fields)
                    stats['text_normalized_count'] += text_normalized

                # Normalize date fields
                if normalize_dates:
                    dates_normalized = self._normalize_date_fields(flat_record)
                    stats['dates_normalized_count'] += dates_normalized

                flattened_data.append(flat_record)
                stats['processed'] += 1

                if context and (i + 1) % 10 == 0:  # Report every 10 records
                    context.report_progress(
                        i + 1,
                        total_records,
                        f"Processed record {i + 1}/{total_records}",
                        {"record_id": flat_record.get('record_id', 'unknown')}
                    )

            except Exception as e:
                logger.error(f"Error processing record {i}: {e}", exc_info=True)
                stats['errors'] += 1

        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write flattened data
        logger.info(f"Writing {len(flattened_data)} flattened records to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(flattened_data, f, ensure_ascii=False, indent=2)

        logger.info(f"Flatten and normalize completed")
        logger.info(f"  Records processed: {stats['processed']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"  Text fields normalized: {stats['text_normalized_count']}")
        logger.info(f"  Date fields normalized: {stats['dates_normalized_count']}")
        logger.info(f"  Numeric fields cleaned: {stats['numeric_fields_cleaned']}")
        logger.info(f"  Field names sanitized: {stats['field_names_sanitized']}")

        if context:
            context.report_progress(
                total_records,
                total_records,
                "Flatten and normalize complete",
                stats
            )

        return {
            'status': 'success',
            'output_file': output_file,
            'count': len(flattened_data),
            'stats': stats
        }

    def _sanitize_field_name(self, field_name: str) -> str:
        """
        Sanitize field name to be JSON/database friendly.
        
        - Removes accents (ó → o, ñ → n, etc.)
        - Converts to lowercase
        - Replaces spaces and special chars with underscores
        - Removes consecutive underscores
        - Removes leading/trailing underscores
        
        Args:
            field_name: Original field name
            
        Returns:
            Sanitized field name
        """
        # FIRST: Remove accents (this is what was missing!)
        sanitized = unidecode(field_name)
        
        # Convert to lowercase
        sanitized = sanitized.lower()
        
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^a-z0-9_]', '_', sanitized)
        
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        return sanitized

    def _clean_numeric_fields(self, record: Dict[str, Any]) -> int:
        """
        Clean numeric fields by removing unnecessary decimal points.
        Example: "123.0" -> "123"
        
        Args:
            record: Record to clean
            
        Returns:
            Number of fields cleaned
        """
        cleaned_count = 0
        
        for key, value in list(record.items()):
            if not isinstance(value, str):
                continue
            
            # Try to convert to float and check if it's a whole number
            try:
                float_value = float(value)
                if float_value.is_integer():
                    record[key] = str(int(float_value))
                    cleaned_count += 1
            except (ValueError, TypeError):
                # Not a numeric string, skip
                pass
        
        return cleaned_count

    def _normalize_text_fields(
        self,
        record: Dict[str, Any],
        uppercase_fields: List[str],
        titlecase_fields: List[str]
    ) -> int:
        """
        Normalize text fields: remove accents and uppercase ALL values except emails.

        Args:
            record: Record to normalize
            uppercase_fields: List of field patterns to uppercase (DEPRECATED - all fields uppercased except emails)
            titlecase_fields: List of field patterns to titlecase (DEPRECATED - all fields uppercased except emails)

        Returns:
            Number of fields normalized
        """
        normalized_count = 0

        # Email field patterns
        email_patterns = ['email', 'correo', 'mail', 'e_mail', 'e-mail']
        
        # Email regex pattern to detect email values
        email_regex = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

        for key, value in list(record.items()):
            if not isinstance(value, str) or not value:
                continue

            original_value = value

            # Remove accents
            value = unidecode(value)
            
            # Check if this is an email field (by field name or value pattern)
            key_lower = key.lower()
            is_email_field = any(pattern in key_lower for pattern in email_patterns)
            is_email_value = email_regex.match(value.strip())
            
            if is_email_field or is_email_value:
                # Keep email values as lowercase (standard for emails)
                value = value.lower()
            else:
                # Uppercase ALL other text values
                value = value.upper()

            # Update record if value changed
            if value != original_value:
                record[key] = value
                normalized_count += 1

        return normalized_count

    def _normalize_date_fields(self, record: Dict[str, Any]) -> int:
        """
        Normalize date fields to ISO format (YYYY-MM-DD).

        Args:
            record: Record to normalize

        Returns:
            Number of date fields normalized
        """
        normalized_count = 0

        # Common date field patterns
        date_patterns = ['fecha', 'date', 'timestamp', 'created', 'updated', 'modified']

        for key, value in list(record.items()):
            if not isinstance(value, str) or not value:
                continue

            key_lower = key.lower()

            # Check if this looks like a date field
            if any(pattern in key_lower for pattern in date_patterns):
                try:
                    # Try to parse the date
                    parsed_date = date_parser.parse(value, fuzzy=False)

                    # Convert to ISO format (YYYY-MM-DD)
                    iso_date = parsed_date.strftime('%Y-%m-%d')

                    if iso_date != value:
                        record[key] = iso_date
                        normalized_count += 1

                except (ValueError, TypeError, date_parser.ParserError) as e:
                    # If parsing fails, keep original value
                    logger.debug(f"Could not parse date field '{key}' with value '{value}': {e}")
                    pass

        return normalized_count