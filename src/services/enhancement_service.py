"""
Enhancement Service - Geocoding and AI Summarization
"""
from typing import Dict, Any, List, Optional
import logging
import json
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
from openai import OpenAI
import os

logger = logging.getLogger(__name__)


class EnhancementService:
    """Service for data enhancement: geocoding and AI summarization"""
    
    def __init__(self):
        self.geocoder = Nominatim(user_agent="asidelco-explorer")
        self.geocode_cache = {}
        self.cache_file = None
        
        # Initialize OpenAI client (will be set when needed)
        self._openai_client = None
    
    def _get_openai_client(self) -> Optional[OpenAI]:
        """Lazy load OpenAI client"""
        if self._openai_client is None:
            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                logger.warning("OPENAI_API_KEY not set, summarization will be skipped")
                return None
            self._openai_client = OpenAI(api_key=api_key)
        return self._openai_client
    
    def _load_geocode_cache(self, cache_path: Path):
        """Load geocode cache from file"""
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    self.geocode_cache = json.load(f)
                logger.info(f"Loaded {len(self.geocode_cache)} cached geocode entries")
            except Exception as e:
                logger.warning(f"Failed to load geocode cache: {e}")
                self.geocode_cache = {}
    
    def _save_geocode_cache(self, cache_path: Path):
        """Save geocode cache to file"""
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.geocode_cache, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.geocode_cache)} geocode entries to cache")
        except Exception as e:
            logger.warning(f"Failed to save geocode cache: {e}")
    
    def _geocode_address(self, address: str, max_retries: int = 3) -> Optional[Dict[str, float]]:
        """
        Geocode an address to lat/lon coordinates.
        
        Args:
            address: Address string to geocode
            max_retries: Maximum retry attempts
            
        Returns:
            Dict with 'latitude' and 'longitude' or None if failed
        """
        # Check cache first
        if address in self.geocode_cache:
            return self.geocode_cache[address]
        
        # Try geocoding with retries
        for attempt in range(max_retries):
            try:
                logger.debug(f"Geocoding: {address} (attempt {attempt + 1}/{max_retries})")
                location = self.geocoder.geocode(address, timeout=10)
                
                if location:
                    result = {
                        'latitude': location.latitude,
                        'longitude': location.longitude
                    }
                    # Cache the result
                    self.geocode_cache[address] = result
                    return result
                else:
                    logger.debug(f"No geocode result for: {address}")
                    self.geocode_cache[address] = None
                    return None
                    
            except GeocoderTimedOut:
                logger.warning(f"Geocoding timeout for: {address} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait before retry
                    continue
                else:
                    self.geocode_cache[address] = None
                    return None
                    
            except GeocoderServiceError as e:
                logger.error(f"Geocoding service error for {address}: {e}")
                self.geocode_cache[address] = None
                return None
                
            except Exception as e:
                logger.error(f"Unexpected geocoding error for {address}: {e}")
                self.geocode_cache[address] = None
                return None
        
        return None
    
    def add_geocoding(
        self,
        input_file: str,
        output_file: str,
        address_field: str = "project_direccion_exacta",
        province_field: str = "project_provincia",
        canton_field: str = "project_canton",
        district_field: str = "project_distrito",
        country: str = "Costa Rica",
        rate_limit: float = 1.0,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Add geocoding (latitude/longitude) to records based on address fields.
        
        Args:
            input_file: Path to input JSON file
            output_file: Path to output JSON file with geocoding
            address_field: Field containing street address
            province_field: Field containing province
            canton_field: Field containing canton
            district_field: Field containing district
            country: Country name to append to address
            rate_limit: Seconds to wait between geocoding requests
            context: Optional context for progress reporting
            
        Returns:
            Dict with status, count, and stats
        """
        logger.info("Starting geocoding enhancement")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Address field: {address_field}")
        logger.info(f"  Rate limit: {rate_limit}s")
        
        # Setup cache
        input_path = Path(input_file)
        cache_dir = input_path.parent
        self.cache_file = cache_dir / "geocode_cache.json"
        self._load_geocode_cache(self.cache_file)
        
        # Load input data
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        with open(input_file, 'r', encoding='utf-8') as f:
            records = json.load(f)
        
        total_records = len(records)
        logger.info(f"Loaded {total_records} records")
        
        if context:
            context.report_progress(0, total_records, "Starting geocoding")
        
        stats = {
            'total_records': total_records,
            'geocoded': 0,
            'cached': 0,
            'failed': 0,
            'skipped': 0
        }
        
        enhanced_records = []
        
        for i, record in enumerate(records):
            try:
                # Build address string from available fields
                address_parts = []
                
                # Add street address
                if address_field in record and record[address_field]:
                    address_parts.append(str(record[address_field]))
                
                # Add district
                if district_field in record and record[district_field]:
                    address_parts.append(str(record[district_field]))
                
                # Add canton
                if canton_field in record and record[canton_field]:
                    address_parts.append(str(record[canton_field]))
                
                # Add province
                if province_field in record and record[province_field]:
                    address_parts.append(str(record[province_field]))
                
                # Add country
                if country:
                    address_parts.append(country)
                
                if not address_parts:
                    logger.debug(f"Record {i}: No address fields available")
                    record['geocoding_status'] = 'no_address'
                    stats['skipped'] += 1
                    enhanced_records.append(record)
                    continue
                
                full_address = ", ".join(address_parts)
                
                # Check if already in cache
                was_cached = full_address in self.geocode_cache
                
                # Geocode
                geocode_result = self._geocode_address(full_address)
                
                if geocode_result:
                    record['latitude'] = geocode_result['latitude']
                    record['longitude'] = geocode_result['longitude']
                    record['geocoded_address'] = full_address
                    record['geocoding_status'] = 'success'
                    
                    if was_cached:
                        stats['cached'] += 1
                    else:
                        stats['geocoded'] += 1
                        # Rate limiting only for new geocoding requests
                        time.sleep(rate_limit)
                    
                    logger.debug(f"Record {i}: Geocoded to ({geocode_result['latitude']}, {geocode_result['longitude']})")
                else:
                    record['geocoding_status'] = 'failed'
                    stats['failed'] += 1
                    logger.debug(f"Record {i}: Geocoding failed for {full_address}")
                
                enhanced_records.append(record)
                
                # Report progress
                if context and (i + 1) % 10 == 0:
                    context.report_progress(
                        i + 1,
                        total_records,
                        f"Geocoded {i + 1}/{total_records} records",
                        {"geocoded": stats['geocoded'], "cached": stats['cached'], "failed": stats['failed']}
                    )
                    
            except Exception as e:
                logger.error(f"Error processing record {i}: {e}", exc_info=True)
                record['geocoding_status'] = 'error'
                stats['failed'] += 1
                enhanced_records.append(record)
        
        # Save enhanced data
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_records, f, ensure_ascii=False, indent=2)
        
        # Save cache
        self._save_geocode_cache(self.cache_file)
        
        logger.info("Geocoding enhancement completed")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  Newly geocoded: {stats['geocoded']}")
        logger.info(f"  From cache: {stats['cached']}")
        logger.info(f"  Failed: {stats['failed']}")
        logger.info(f"  Skipped: {stats['skipped']}")
        
        if context:
            context.report_progress(
                total_records,
                total_records,
                "Geocoding complete",
                stats
            )
        
        return {
            'status': 'success',
            'output_file': output_file,
            'count': len(enhanced_records),
            'stats': stats
        }
    
    def generate_summaries(
        self,
        input_file: str,
        output_file: str,
        source_fields: Optional[List[str]] = None,
        summary_field: str = "resumen",
        model: str = "gpt-4o-mini",
        max_tokens: int = 300,
        temperature: float = 0.3,
        skip_existing: bool = True,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Generate AI summaries for records using OpenAI.
        
        Args:
            input_file: Path to input JSON file
            output_file: Path to output JSON file with summaries
            source_fields: List of field names to include in summary (defaults to key project fields)
            summary_field: Name of field to store summary
            model: OpenAI model to use
            max_tokens: Maximum tokens for summary
            temperature: Model temperature (0-1, lower = more focused)
            skip_existing: Skip records that already have summaries
            context: Optional context for progress reporting
            
        Returns:
            Dict with status, count, and stats
        """
        logger.info("Starting AI summarization")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Model: {model}")
        logger.info(f"  Summary field: {summary_field}")
        
        # Get OpenAI client
        client = self._get_openai_client()
        if not client:
            logger.error("OpenAI client not available - check OPENAI_API_KEY")
            return {
                'status': 'error',
                'message': 'OpenAI API key not configured',
                'count': 0,
                'stats': {}
            }
        
        # Default source fields if not provided
        if source_fields is None:
            source_fields = [
                'project_descripcion_del_proyecto',
                'project_clasificacion',
                'project_direccion_exacta',
                'project_provincia',
                'project_canton',
                'project_distrito',
                'project_area_de_construccion',
                'project_uso',
                'professional_nombre',
                'professional_apellido1',
                'professional_apellido2'
            ]
        
        # Load input data
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        with open(input_file, 'r', encoding='utf-8') as f:
            records = json.load(f)
        
        total_records = len(records)
        logger.info(f"Loaded {total_records} records")
        
        if context:
            context.report_progress(0, total_records, "Starting summarization")
        
        stats = {
            'total_records': total_records,
            'summarized': 0,
            'skipped': 0,
            'failed': 0
        }
        
        enhanced_records = []
        
        # System prompt for summarization
        system_prompt = """Eres un asistente que crea resúmenes concisos de proyectos de construcción en Costa Rica.
Genera un resumen en español que incluya:
- Tipo de proyecto y clasificación
- Ubicación (provincia, cantón, distrito, dirección)
- Características principales (área, uso)
- Profesional responsable

El resumen debe ser claro, informativo y no más de 3-4 oraciones."""
        
        for i, record in enumerate(records):
            try:
                # Skip if summary already exists and skip_existing is True
                if skip_existing and summary_field in record and record[summary_field]:
                    logger.debug(f"Record {i}: Skipping - summary already exists")
                    stats['skipped'] += 1
                    enhanced_records.append(record)
                    continue
                
                # Build context from source fields
                context_parts = []
                for field in source_fields:
                    if field in record and record[field]:
                        # Format field name nicely
                        field_name = field.replace('project_', '').replace('professional_', '').replace('_', ' ').title()
                        context_parts.append(f"{field_name}: {record[field]}")
                
                if not context_parts:
                    logger.debug(f"Record {i}: No source fields available")
                    stats['skipped'] += 1
                    enhanced_records.append(record)
                    continue
                
                # Create user prompt
                user_prompt = "Información del proyecto:\n\n" + "\n".join(context_parts)
                
                # Generate summary
                logger.debug(f"Record {i}: Generating summary")
                
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=temperature
                )
                
                summary = response.choices[0].message.content.strip()
                
                # Add summary to record
                record[summary_field] = summary
                record['summary_model'] = model
                record['summary_tokens'] = response.usage.total_tokens
                
                stats['summarized'] += 1
                logger.debug(f"Record {i}: Summary generated ({response.usage.total_tokens} tokens)")
                
                enhanced_records.append(record)
                
                # Report progress
                if context and (i + 1) % 5 == 0:  # Report every 5 records (API calls are slower)
                    context.report_progress(
                        i + 1,
                        total_records,
                        f"Summarized {i + 1}/{total_records} records",
                        {"summarized": stats['summarized'], "skipped": stats['skipped'], "failed": stats['failed']}
                    )
                
                # Rate limiting for API calls (be respectful)
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing record {i}: {e}", exc_info=True)
                stats['failed'] += 1
                enhanced_records.append(record)
        
        # Save enhanced data
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_records, f, ensure_ascii=False, indent=2)
        
        logger.info("AI summarization completed")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  Summarized: {stats['summarized']}")
        logger.info(f"  Skipped: {stats['skipped']}")
        logger.info(f"  Failed: {stats['failed']}")
        
        if context:
            context.report_progress(
                total_records,
                total_records,
                "Summarization complete",
                stats
            )
        
        return {
            'status': 'success',
            'output_file': output_file,
            'count': len(enhanced_records),
            'stats': stats
        }