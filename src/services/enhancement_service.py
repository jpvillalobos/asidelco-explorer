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

    CR_PROVINCE_CENTROIDS = {
        "SAN JOSE": {"latitude": 9.9281, "longitude": -84.0907},
        "ALAJUELA": {"latitude": 10.0162, "longitude": -84.2116},
        "CARTAGO": {"latitude": 9.8644, "longitude": -83.9194},
        "HEREDIA": {"latitude": 10.0024, "longitude": -84.1165},
        "GUANACASTE": {"latitude": 10.6267, "longitude": -85.4437},
        "PUNTARENAS": {"latitude": 9.9763, "longitude": -84.8384},
        "LIMON": {"latitude": 9.9896, "longitude": -83.0350},
    }
    
    def __init__(self):
        self.geocoder = Nominatim(user_agent="asidelco-explorer")
        self.geocode_cache = {}
        self.cache_file = None
        
        # Initialize OpenAI client (will be set when needed)
        self._openai_client = None

    def _first_value(self, record: Dict[str, Any], *keys: str) -> str:
        """Return the first meaningful value from flat or nested record keys."""
        for key in keys:
            value = record.get(key)
            if value not in (None, "", "NO REGISTRADO"):
                return str(value).strip()

        sections = {
            "csv": record.get("csv_data", {}) or {},
            "project": record.get("project_data", {}) or {},
            "professional": record.get("professional_data", {}) or {},
        }
        for key in keys:
            if "_" not in key:
                continue
            section_name, section_key = key.split("_", 1)
            section = sections.get(section_name, {})
            value = section.get(section_key)
            if value not in (None, "", "NO REGISTRADO"):
                return str(value).strip()

        return ""

    def build_project_search_summary(self, record: Dict[str, Any]) -> str:
        """
        Build deterministic project text for embeddings and search.

        This intentionally favors exact names, IDs, classifications, and location
        fields over prose so vector search has the important tokens available.
        """
        record_id = self._first_value(record, "record_id", "csv_id")
        project_id = self._first_value(record, "project_num_proyecto", "project_project_id", "csv_proyecto")
        description = self._first_value(
            record,
            "project_descripcion_del_proyecto",
            "project_descripcion",
            "csv_subobra"
        )
        classification = self._first_value(record, "project_clasificacion", "csv_clasificacion")
        obra = self._first_value(record, "csv_obra")
        subobra = self._first_value(record, "csv_subobra", "project_descripcion")
        province = self._first_value(record, "project_provincia", "csv_provincia")
        canton = self._first_value(record, "project_canton", "csv_canton")
        district = self._first_value(record, "project_distrito", "csv_distrito")
        address = self._first_value(record, "project_direccion_exacta")
        area = self._first_value(record, "csv_area", "project_area_de_construccion")
        unit = self._first_value(record, "csv_unidad")
        tasado = self._first_value(record, "project_tasado")
        estado = self._first_value(record, "project_estado")
        owner = self._first_value(record, "project_nombre_propietario")
        project_responsible = self._first_value(record, "project_responsable")
        professional_name = self._first_value(record, "professional_nombrecompleto")
        professional_id = self._first_value(record, "professional_cedula")
        professional_license = self._first_value(
            record,
            "professional_carne",
            "professional_carnet",
            "project_carnet_profesional"
        )
        professional_college = self._first_value(record, "professional_colegio")
        professional_email = self._first_value(record, "professional_correopermanente", "professional_correolaboral")

        parts = []
        if record_id or project_id:
            ids = ", ".join(value for value in [record_id, project_id] if value)
            parts.append(f"Proyecto {ids}.")
        if description:
            parts.append(f"Descripcion del proyecto: {description}.")
        classifications = ", ".join(value for value in [classification, obra, subobra] if value)
        if classifications:
            parts.append(f"Clasificacion: {classifications}.")
        location = ", ".join(value for value in [district, canton, province] if value)
        if location:
            parts.append(f"Ubicacion: {location}.")
        if address:
            parts.append(f"Direccion exacta: {address}.")
        if area:
            area_text = f"{area} {unit}".strip()
            parts.append(f"Area: {area_text}.")
        if tasado:
            parts.append(f"Monto tasado: {tasado}.")
        if estado:
            parts.append(f"Estado del tramite: {estado}.")
        if owner:
            parts.append(f"Propietario: {owner}.")
        if project_responsible:
            parts.append(f"Responsable del proyecto o empresa: {project_responsible}.")
        if professional_name:
            parts.append(f"Profesional responsable: {professional_name}.")
        if professional_id:
            parts.append(f"Cedula profesional: {professional_id}.")
        if professional_license:
            parts.append(f"Carne profesional: {professional_license}.")
        if professional_college:
            parts.append(f"Colegio profesional: {professional_college}.")
        if professional_email:
            parts.append(f"Correo profesional: {professional_email}.")

        return " ".join(parts)

    def _summary_has_placeholders(self, summary: str) -> bool:
        """Detect summaries that contain unresolved LLM/template placeholders."""
        if not summary:
            return False

        placeholder_markers = [
            "[nombre del profesional]",
            "[numero de identificacion",
            "[número de identificación",
            "[número",
            "[numero",
            "[",
            "]",
        ]
        normalized = summary.lower()
        return any(marker in normalized for marker in placeholder_markers)

    def _normalize_geo_text(self, value: Any) -> str:
        if value in (None, "", "NO REGISTRADO"):
            return ""
        return str(value).strip()

    def _record_geo_value(self, record: Dict[str, Any], field: str) -> Optional[str]:
        value = self._normalize_geo_text(record.get(field))
        if value:
            return value

        fallback_pairs = {
            "project_provincia": "csv_provincia",
            "project_canton": "csv_canton",
            "project_distrito": "csv_distrito",
        }
        fallback_field = fallback_pairs.get(field)
        if fallback_field:
            value = self._normalize_geo_text(record.get(fallback_field))
            if value:
                return value

        return None

    def _province_centroid(self, province: Optional[str]) -> Optional[Dict[str, Any]]:
        province_key = self._normalize_geo_text(province).upper()
        centroid = self.CR_PROVINCE_CENTROIDS.get(province_key)
        if not centroid:
            return None

        return {
            "latitude": centroid["latitude"],
            "longitude": centroid["longitude"],
            "geocoding_level": 5,
            "geocoding_description": "Province centroid",
            "geocoding_precision": "province",
            "geocoded_address": f"{province_key}, Costa Rica",
            "geocoding_source": "local_admin_centroid",
        }

    def apply_location_field(self, record: Dict[str, Any]) -> bool:
        """
        Add OpenSearch-compatible geo_point field from latitude/longitude.

        OpenSearch Dashboards Maps can use this object directly when mapped as
        geo_point:
          {"lat": 10.0, "lon": -84.0}
        """
        latitude = record.get("latitude")
        longitude = record.get("longitude")
        if latitude in (None, "") or longitude in (None, ""):
            return False

        try:
            lat = float(latitude)
            lon = float(longitude)
        except (TypeError, ValueError):
            return False

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return False

        record["location"] = {
            "lat": lat,
            "lon": lon
        }
        return True
    
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
                negative_entries = [
                    key for key, value in self.geocode_cache.items()
                    if not value
                ]
                for key in negative_entries:
                    self.geocode_cache.pop(key, None)
                logger.info(
                    "Loaded %s cached geocode entries (%s stale negative entries ignored)",
                    len(self.geocode_cache),
                    len(negative_entries),
                )
            except Exception as e:
                logger.warning(f"Failed to load geocode cache: {e}")
                self.geocode_cache = {}
    
    def _save_geocode_cache(self, cache_path: Path):
        """Save geocode cache to file"""
        try:
            self.geocode_cache = {
                key: value for key, value in self.geocode_cache.items()
                if value
            }
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.geocode_cache, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.geocode_cache)} geocode entries to cache")
        except Exception as e:
            logger.warning(f"Failed to save geocode cache: {e}")
    
    def _geocode_address(
        self,
        address: str,
        max_retries: int = 3,
        allow_external: bool = True,
    ) -> Optional[Dict[str, float]]:
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
            cached = self.geocode_cache[address]
            if cached:
                result = dict(cached)
                result['from_cache'] = True
                return result
            self.geocode_cache.pop(address, None)

        if not allow_external:
            return None

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
                    return None

            except GeocoderTimedOut:
                logger.warning(f"Geocoding timeout for: {address} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait before retry
                    continue
                else:
                    return None

            except GeocoderServiceError as e:
                logger.error(f"Geocoding service error for {address}: {e}")
                return None

            except Exception as e:
                logger.error(f"Unexpected geocoding error for {address}: {e}")
                return None

        return None

    def _geocode_with_fallback(
        self,
        street: Optional[str],
        district: Optional[str],
        canton: Optional[str],
        province: Optional[str],
        country: str,
        max_retries: int = 3,
        allow_external: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Geocode an address with multi-level fallback strategy.

        Tries progressively broader geographic areas:
        1. Full address (street + district + canton + province + country)
        2. District + canton + province + country
        3. Canton + province + country
        4. Province + country

        Args:
            street: Street address (may contain Costa Rican meter-based directions)
            district: District name
            canton: Canton name
            province: Province name
            country: Country name
            max_retries: Maximum retry attempts per level

        Returns:
            Dict with 'latitude', 'longitude', and 'geocoding_level' or None if all levels failed
        """
        # Build address combinations for each fallback level
        fallback_levels = []

        # Level 1: Full address with street
        if street and district and canton and province:
            level1_parts = [street, district, canton, province, country]
            fallback_levels.append({
                'level': 1,
                'address': ', '.join(level1_parts),
                'description': 'Full address'
            })

        # Level 2: District + canton + province + country (skip problematic street)
        if district and canton and province:
            level2_parts = [district, canton, province, country]
            fallback_levels.append({
                'level': 2,
                'address': ', '.join(level2_parts),
                'description': 'District level'
            })

        # Level 3: Canton + province + country
        if canton and province:
            level3_parts = [canton, province, country]
            fallback_levels.append({
                'level': 3,
                'address': ', '.join(level3_parts),
                'description': 'Canton level'
            })

        # Level 4: Province + country
        if province:
            level4_parts = [province, country]
            fallback_levels.append({
                'level': 4,
                'address': ', '.join(level4_parts),
                'description': 'Province level'
            })

        # Try each level until one succeeds
        for fallback in fallback_levels:
            address = fallback['address']
            level = fallback['level']
            description = fallback['description']

            logger.debug(f"Trying geocoding level {level} ({description}): {address}")

            result = self._geocode_address(
                address,
                max_retries=max_retries,
                allow_external=allow_external,
            )

            if result:
                result['geocoding_level'] = level
                result['geocoding_description'] = description
                result['geocoding_precision'] = {
                    1: 'exact_address',
                    2: 'district',
                    3: 'canton',
                    4: 'province',
                }.get(level, 'unknown')
                result['geocoded_address'] = address
                result.setdefault('geocoding_source', 'nominatim')
                logger.debug(f"Geocoding succeeded at level {level} ({description})")
                return result
            else:
                logger.debug(f"Geocoding failed at level {level} ({description})")

        centroid = self._province_centroid(province)
        if centroid:
            logger.debug(f"Using local province centroid fallback for: {province}")
            return centroid

        # All levels failed
        logger.debug(f"All geocoding levels failed for: street={street}, district={district}, canton={canton}, province={province}")
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
            'skipped': 0,
            'level_1': 0,  # Full address
            'level_2': 0,  # District level
            'level_3': 0,  # Canton level
            'level_4': 0,  # Province level
            'level_5': 0   # Local province centroid
        }
        
        enhanced_records = []
        
        for i, record in enumerate(records):
            try:
                # Extract address components
                street = self._record_geo_value(record, address_field)
                district = self._record_geo_value(record, district_field)
                canton = self._record_geo_value(record, canton_field)
                province = self._record_geo_value(record, province_field)

                # Check if we have at least province (minimum required)
                if not province:
                    logger.debug(f"Record {i}: No province field available, skipping geocoding")
                    record['geocoding_status'] = 'no_address'
                    stats['skipped'] += 1
                    enhanced_records.append(record)
                    continue

                # Use fallback geocoding strategy
                geocode_result = self._geocode_with_fallback(
                    street=street,
                    district=district,
                    canton=canton,
                    province=province,
                    country=country,
                    max_retries=3,
                    allow_external=True,
                )

                if geocode_result:
                    fallback_address = ", ".join(
                        p for p in [street, district, canton, province, country] if p
                    )
                    record['latitude'] = geocode_result['latitude']
                    record['longitude'] = geocode_result['longitude']
                    self.apply_location_field(record)
                    record['geocoded_address'] = geocode_result.get('geocoded_address', fallback_address)
                    record['geocoding_level'] = geocode_result.get('geocoding_level', 0)
                    record['geocoding_description'] = geocode_result.get('geocoding_description', 'Unknown')
                    record['geocoding_precision'] = geocode_result.get('geocoding_precision', 'unknown')
                    record['geocoding_source'] = geocode_result.get('geocoding_source', 'unknown')
                    record['geocoding_status'] = 'success'

                    # Track level statistics
                    level = geocode_result.get('geocoding_level', 0)
                    if level in [1, 2, 3, 4, 5]:
                        stats[f'level_{level}'] += 1

                    if geocode_result.get('from_cache'):
                        stats['cached'] += 1
                    elif geocode_result.get('geocoding_source') == 'local_admin_centroid':
                        stats['geocoded'] += 1
                    else:
                        stats['geocoded'] += 1
                        # Rate limiting only for new geocoding requests
                        time.sleep(rate_limit)

                    logger.debug(f"Record {i}: Geocoded to ({geocode_result['latitude']}, {geocode_result['longitude']}) at level {level}")
                else:
                    record['geocoding_status'] = 'failed'
                    stats['failed'] += 1
                    logger.debug(f"Record {i}: Geocoding failed for all levels")

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
        logger.info(f"  Geocoding levels breakdown:")
        logger.info(f"    Level 1 (Full address): {stats['level_1']}")
        logger.info(f"    Level 2 (District): {stats['level_2']}")
        logger.info(f"    Level 3 (Canton): {stats['level_3']}")
        logger.info(f"    Level 4 (Province): {stats['level_4']}")
        logger.info(f"    Level 5 (Local province centroid): {stats['level_5']}")
        
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

    def repair_missing_geocoding(
        self,
        input_file: str,
        output_file: str,
        address_field: str = "project_direccion_exacta",
        province_field: str = "project_provincia",
        canton_field: str = "project_canton",
        district_field: str = "project_distrito",
        country: str = "Costa Rica",
        rate_limit: float = 1.0,
        allow_external: bool = False,
        chunk_size: int = 1024 * 1024,
        progress_interval: int = 10000,
    ) -> Dict[str, Any]:
        """
        Repair records missing a valid OpenSearch geo_point without loading all
        records in memory.

        Existing valid coordinates are preserved. Missing records are retried
        with the same fallback strategy used by the geocoding stage, including
        csv_* location fallbacks and local province centroids.
        """
        from services.validation_enrichment_service import ValidationEnrichmentService

        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.cache_file = input_path.parent / "geocode_cache.json"
        self._load_geocode_cache(self.cache_file)

        reader = ValidationEnrichmentService()
        stats = {
            "processed": 0,
            "already_geocoded": 0,
            "repaired": 0,
            "cached": 0,
            "failed": 0,
            "skipped": 0,
            "missing_after": 0,
            "level_1": 0,
            "level_2": 0,
            "level_3": 0,
            "level_4": 0,
            "level_5": 0,
        }

        started_at = time.time()
        first_record = True
        with open(output_path, "w", encoding="utf-8") as out:
            out.write("[\n")

            for record in reader._iter_json_array(input_path, chunk_size=chunk_size):
                stats["processed"] += 1

                if self.apply_location_field(record):
                    stats["already_geocoded"] += 1
                else:
                    record.pop("location", None)
                    street = self._record_geo_value(record, address_field)
                    district = self._record_geo_value(record, district_field)
                    canton = self._record_geo_value(record, canton_field)
                    province = self._record_geo_value(record, province_field)

                    if not province:
                        record["geocoding_status"] = "no_address"
                        stats["skipped"] += 1
                    else:
                        geocode_result = self._geocode_with_fallback(
                            street=street,
                            district=district,
                            canton=canton,
                            province=province,
                            country=country,
                            max_retries=3,
                            allow_external=allow_external,
                        )

                        if geocode_result:
                            record["latitude"] = geocode_result["latitude"]
                            record["longitude"] = geocode_result["longitude"]
                            self.apply_location_field(record)
                            record["geocoded_address"] = geocode_result.get(
                                "geocoded_address",
                                ", ".join(p for p in [street, district, canton, province, country] if p),
                            )
                            record["geocoding_level"] = geocode_result.get("geocoding_level", 0)
                            record["geocoding_description"] = geocode_result.get(
                                "geocoding_description",
                                "Unknown",
                            )
                            record["geocoding_precision"] = geocode_result.get(
                                "geocoding_precision",
                                "unknown",
                            )
                            record["geocoding_source"] = geocode_result.get(
                                "geocoding_source",
                                "unknown",
                            )
                            record["geocoding_status"] = "success"
                            stats["repaired"] += 1

                            level = geocode_result.get("geocoding_level", 0)
                            if level in [1, 2, 3, 4, 5]:
                                stats[f"level_{level}"] += 1

                            if geocode_result.get("from_cache"):
                                stats["cached"] += 1
                            elif geocode_result.get("geocoding_source") != "local_admin_centroid":
                                time.sleep(rate_limit)
                        else:
                            record["geocoding_status"] = "failed"
                            stats["failed"] += 1

                    if not self.apply_location_field(record):
                        stats["missing_after"] += 1

                if not first_record:
                    out.write(",\n")
                json.dump(record, out, ensure_ascii=False)
                first_record = False

                if progress_interval and stats["processed"] % progress_interval == 0:
                    elapsed = max(time.time() - started_at, 0.001)
                    rate = stats["processed"] / elapsed
                    logger.info(
                        "Processed %s; repaired %s; missing after %s; %.2f records/sec",
                        stats["processed"],
                        stats["repaired"],
                        stats["missing_after"],
                        rate,
                    )

            out.write("\n]\n")

        self._save_geocode_cache(self.cache_file)

        logger.info("Missing geocoding repair completed")
        logger.info(f"  Records processed: {stats['processed']}")
        logger.info(f"  Already geocoded: {stats['already_geocoded']}")
        logger.info(f"  Repaired: {stats['repaired']}")
        logger.info(f"  Missing after repair: {stats['missing_after']}")

        return {
            "status": "success",
            "output_file": str(output_path),
            "count": stats["processed"],
            "stats": stats,
        }
    
    def generate_summaries(
        self,
        input_file: str,
        output_file: str,
        source_fields: Optional[List[str]] = None,
        summary_field: str = "resumen",
        model: str = "gpt-5-nano",
        max_completion_tokens: int = 300,
        temperature: float = 0.3,
        skip_existing: bool = True,
        use_ai: bool = False,
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
            max_completion_tokens: Maximum tokens for summary
            temperature: Model temperature (0-1, lower = more focused)
            skip_existing: Skip records that already have summaries
            use_ai: Whether to use OpenAI chat completions. False uses deterministic summaries.
            context: Optional context for progress reporting
            
        Returns:
            Dict with status, count, and stats
        """
        logger.info("Starting AI summarization")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Model: {model}")
        logger.info(f"  Summary field: {summary_field}")
        logger.info(f"  Use AI: {use_ai}")
        
        client = None
        if use_ai:
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
                'csv_area',
                'csv_unidad',
                'project_area_de_construccion',
                'project_uso',
                'project_responsable',
                'professional_nombre',
                'professional_apellido1',
                'professional_apellido2',
                'professional_nombrecompleto',
                'professional_cedula',
                'professional_carne',
                'professional_carnet',
                'professional_colegio'
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
- Identificación y carné del profesional responsable

El resumen debe ser claro, informativo y no más de 3-4 oraciones.
El resumen deber estar orientado a facilitar el procesamiento de embeddings para este campo"""
        
        for i, record in enumerate(records):
            try:
                # Skip if summary already exists and skip_existing is True
                existing_summary = record.get(summary_field, "")
                if skip_existing and existing_summary and not self._summary_has_placeholders(str(existing_summary)):
                    logger.debug(f"Record {i}: Skipping - summary already exists")
                    stats['skipped'] += 1
                    enhanced_records.append(record)
                    continue

                if not use_ai:
                    summary = self.build_project_search_summary(record)
                    if not summary:
                        logger.debug(f"Record {i}: No fields available for deterministic summary")
                        stats['skipped'] += 1
                        enhanced_records.append(record)
                        continue

                    record[summary_field] = summary
                    record['summary_model'] = 'deterministic-search-summary-v1'
                    record['summary_tokens'] = None
                    stats['summarized'] += 1
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
                    max_completion_tokens=max_completion_tokens,
                    temperature=temperature
                )
                
                summary = response.choices[0].message.content.strip()
                if self._summary_has_placeholders(summary):
                    logger.warning(f"Record {i}: AI summary contained placeholders; using deterministic fallback")
                    summary = self.build_project_search_summary(record)
                
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
