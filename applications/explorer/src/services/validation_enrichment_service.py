"""
Validation and Enrichment Service
"""
from typing import Dict, Any, List, Optional, Set
from pathlib import Path
import logging
import json
import re
from datetime import datetime
import unicodedata

logger = logging.getLogger(__name__)


class ValidationEnrichmentService:
    """Service for validating and enriching merged data"""
    
    def __init__(self):
        """Initialize validation service"""
        self.stats = {
            "records_processed": 0,
            "records_valid": 0,
            "records_invalid": 0,
            "validation_errors": 0,
            "enrichments_added": 0
        }
        
        # Valid values for categorical fields
        self.VALID_ESTADOS = {
            "Permiso de Construcción",
            "En Revisión",
            "Aprobado",
            "Rechazado",
            "Pendiente",
            "Anulado"
        }
        
        self.VALID_OBRAS = {
            "HABITACIONAL",
            "COMERCIAL",
            "INDUSTRIAL",
            "TURISTICO",
            "OBRAS COMPLEMENTARIAS",
            "SERVICIOS PUBLICOS"
        }
        
        # Costa Rica provinces
        self.VALID_PROVINCIAS = {
            "SAN JOSE",
            "ALAJUELA",
            "CARTAGO",
            "HEREDIA",
            "GUANACASTE",
            "PUNTARENAS",
            "LIMON"
        }
    
    def validate_and_enrich(
        self,
        input_file: str,
        output_file: str,
        validation_rules: Optional[Dict[str, Any]] = None,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Validate and enrich merged data
        
        Validation checks:
        - Required fields present
        - Data type correctness
        - Value range/format validation
        - Referential integrity
        - Costa Rica-specific validations (provinces, cedulas, etc.)
        
        Enrichments:
        - Normalized text fields
        - Calculated fields (age of project, etc.)
        - Geographic metadata
        - Classification metadata
        - Quality scores
        
        Args:
            input_file: Path to merged JSON file
            output_file: Path to output validated/enriched JSON
            validation_rules: Optional custom validation rules
            context: Optional context for progress reporting
            
        Returns:
            Dictionary with validation/enrichment results
        """
        logger.info("="*80)
        logger.info("Starting validation and enrichment")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info("="*80)
        
        # Load merged data
        if context:
            context.report_progress(0, 100, "Loading merged data")
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                records = json.load(f)
            logger.info(f"✓ Loaded {len(records)} records")
        except Exception as e:
            logger.error(f"Failed to load input file: {e}")
            raise
        
        # Process each record
        if context:
            context.report_progress(10, 100, "Validating and enriching records")
        
        validated_records = []
        total_records = len(records)
        
        for idx, record in enumerate(records):
            self.stats["records_processed"] += 1
            
            # Progress reporting
            if context and idx % 100 == 0:
                progress = 10 + int((idx / total_records) * 80)
                context.report_progress(
                    progress,
                    100,
                    f"Processing record {idx + 1}/{total_records}",
                    {
                        "processed": self.stats["records_processed"],
                        "valid": self.stats["records_valid"],
                        "invalid": self.stats["records_invalid"]
                    }
                )
            
            # Validate and enrich
            validated_record = self._validate_and_enrich_record(
                record,
                validation_rules or {},
                idx
            )
            
            validated_records.append(validated_record)
        
        # Save validated data
        if context:
            context.report_progress(90, 100, f"Saving {len(validated_records)} validated records")
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(validated_records, f, ensure_ascii=False, indent=2)
        
        logger.info("="*80)
        logger.info("Validation and enrichment completed")
        logger.info(f"  Records processed: {self.stats['records_processed']}")
        logger.info(f"  Valid records: {self.stats['records_valid']}")
        logger.info(f"  Invalid records: {self.stats['records_invalid']}")
        logger.info(f"  Validation errors: {self.stats['validation_errors']}")
        logger.info(f"  Enrichments added: {self.stats['enrichments_added']}")
        logger.info(f"  Output file: {output_path}")
        logger.info("="*80)
        
        if context:
            context.report_progress(
                100,
                100,
                f"Validation complete: {len(validated_records)} records",
                self.stats
            )
        
        return {
            "count": len(validated_records),
            "output_file": str(output_path),
            "stats": self.stats.copy()
        }
    
    def _validate_and_enrich_record(
        self,
        record: Dict[str, Any],
        validation_rules: Dict[str, Any],
        record_index: int
    ) -> Dict[str, Any]:
        """
        Validate and enrich a single record
        
        Args:
            record: Merged record dictionary
            validation_rules: Validation rules
            record_index: Record index for logging
            
        Returns:
            Validated and enriched record
        """
        # Create enriched record structure
        enriched = record.copy()
        
        # Initialize validation metadata
        if "validation" not in enriched:
            enriched["validation"] = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "validated_at": datetime.now().isoformat()
            }
        
        # Initialize enrichment container
        if "enrichment" not in enriched:
            enriched["enrichment"] = {}
        
        # Run validations
        self._validate_record(enriched, validation_rules, record_index)
        
        # Run enrichments
        self._enrich_record(enriched, record_index)
        
        # Update stats
        if enriched["validation"]["is_valid"]:
            self.stats["records_valid"] += 1
        else:
            self.stats["records_invalid"] += 1
        
        self.stats["validation_errors"] += len(enriched["validation"]["errors"])
        
        return enriched
    
    def _validate_record(
        self,
        record: Dict[str, Any],
        validation_rules: Dict[str, Any],
        record_index: int
    ):
        """
        Run validation checks on record (modifies in place)
        """
        validation = record["validation"]
        csv_data = record.get("csv_data", {})
        project_data = record.get("project_data", {})
        professional_data = record.get("professional_data", {})
        
        # 1. Required fields validation
        if not csv_data.get("proyecto"):
            validation["errors"].append("Missing required field: proyecto")
            validation["is_valid"] = False
        
        if not csv_data.get("id"):
            validation["errors"].append("Missing required field: id")
            validation["is_valid"] = False
        
        # 2. ID format validation (proyecto-sequence)
        record_id = csv_data.get("id", "")
        if record_id and not re.match(r'^\d+-\d+$', str(record_id)):
            validation["errors"].append(f"Invalid ID format: {record_id} (expected: proyecto-sequence)")
            validation["is_valid"] = False
        
        # 3. Numeric field validation
        area = csv_data.get("area")
        if area is not None:
            try:
                area_num = float(area)
                if area_num <= 0:
                    validation["warnings"].append(f"Area is zero or negative: {area_num}")
            except (ValueError, TypeError):
                validation["errors"].append(f"Invalid area value: {area}")
                validation["is_valid"] = False
        
        # 4. Date format validation
        fecha = csv_data.get("fechaproyecto")
        if fecha:
            if not self._is_valid_date(fecha):
                validation["warnings"].append(f"Unusual date format: {fecha}")
        
        # 5. Categorical field validation
        obra = csv_data.get("obra")
        if obra and str(obra).upper() not in self.VALID_OBRAS:
            validation["warnings"].append(f"Unknown obra type: {obra}")
        
        provincia = csv_data.get("provincia")
        if provincia and str(provincia).upper() not in self.VALID_PROVINCIAS:
            validation["errors"].append(f"Invalid provincia: {provincia}")
            validation["is_valid"] = False
        
        # 6. Project data validation
        if project_data:
            estado = project_data.get("Estado")
            if estado and estado not in self.VALID_ESTADOS:
                validation["warnings"].append(f"Unknown project estado: {estado}")
            
            # Validate Tasado amount
            tasado = project_data.get("Tasado")
            if tasado:
                try:
                    tasado_num = float(tasado)
                    if tasado_num < 0:
                        validation["errors"].append("Tasado amount is negative")
                        validation["is_valid"] = False
                except (ValueError, TypeError):
                    validation["warnings"].append(f"Invalid Tasado format: {tasado}")
        
        # 7. Professional data validation
        if professional_data:
            # Validate Cedula format (Costa Rica)
            cedula = professional_data.get("Cedula")
            if cedula and not self._is_valid_cedula(cedula):
                validation["warnings"].append(f"Invalid cedula format: {cedula}")
            
            # Validate email format
            for email_field in ["CorreoPermanente", "CorreoLaboral"]:
                email = professional_data.get(email_field)
                if email and email != "NO REGISTRADO" and not self._is_valid_email(email):
                    validation["warnings"].append(f"Invalid email in {email_field}: {email}")
        
        # 8. Consistency checks
        # Check if CSV provincia matches project provincia
        csv_prov = str(csv_data.get("provincia", "")).upper()
        proj_prov = str(project_data.get("Provincia", "")).upper()
        if csv_prov and proj_prov and csv_prov != proj_prov:
            validation["warnings"].append(
                f"Provincia mismatch: CSV={csv_prov}, Project={proj_prov}"
            )
        
        logger.debug(f"Record {record_index}: {len(validation['errors'])} errors, {len(validation['warnings'])} warnings")
    
    def _enrich_record(
        self,
        record: Dict[str, Any],
        record_index: int
    ):
        """
        Add enrichments to record (modifies in place)
        """
        enrichment = record["enrichment"]
        csv_data = record.get("csv_data", {})
        project_data = record.get("project_data", {})
        professional_data = record.get("professional_data", {})
        
        # 1. Normalized location
        provincia = csv_data.get("provincia") or project_data.get("Provincia", "")
        canton = csv_data.get("canton") or project_data.get("Cantón", "")
        distrito = csv_data.get("distrito") or project_data.get("Distrito", "")
        
        enrichment["location_normalized"] = {
            "provincia": self._normalize_text(provincia),
            "canton": self._normalize_text(canton),
            "distrito": self._normalize_text(distrito),
            "full_location": f"{distrito}, {canton}, {provincia}".upper() if all([distrito, canton, provincia]) else None
        }
        
        # 2. Project age calculation
        fecha_proyecto = project_data.get("Fecha Proyecto")
        if fecha_proyecto:
            try:
                fecha_dt = self._parse_date(fecha_proyecto)
                if fecha_dt:
                    days_old = (datetime.now() - fecha_dt).days
                    enrichment["project_age_days"] = days_old
                    enrichment["project_age_years"] = round(days_old / 365.25, 2)
            except Exception as e:
                logger.debug(f"Could not calculate project age: {e}")
        
        # 3. Classification metadata
        obra = str(csv_data.get("obra", "")).upper()
        subobra = str(csv_data.get("subobra", "")).upper()
        
        enrichment["classification"] = {
            "category": obra,
            "subcategory": subobra,
            "is_residential": "HABITACIONAL" in obra,
            "is_commercial": "COMERCIAL" in obra,
            "is_social_interest": "INTERES SOCIAL" in subobra,
            "is_exonerated": csv_data.get("exonerado") == "SI"
        }
        
        # 4. Financial metadata
        tasado = project_data.get("Tasado")
        if tasado:
            try:
                tasado_num = float(tasado)
                area_num = float(csv_data.get("area", 0))
                
                enrichment["financial"] = {
                    "tasado_amount": tasado_num,
                    "price_per_m2": round(tasado_num / area_num, 2) if area_num > 0 else None,
                    "is_high_value": tasado_num > 100000000,  # > 100M colones
                    "is_low_value": tasado_num < 10000000     # < 10M colones
                }
            except (ValueError, TypeError):
                pass
        
        # 5. Professional metadata
        if professional_data:
            colegio = professional_data.get("Colegio", "")
            carne = professional_data.get("Carne", "")
            
            enrichment["professional_info"] = {
                "college": colegio,
                "license_prefix": carne.split("-")[0] if "-" in carne else None,
                "is_architect": "ARQUITECTO" in colegio.upper(),
                "is_engineer": "INGENIERO" in colegio.upper() or "ICO" in carne,
                "has_company": bool(professional_data.get("Lugar"))
            }
        
        # 6. Data completeness score
        total_fields = 0
        filled_fields = 0
        
        for data_dict in [csv_data, project_data, professional_data]:
            for value in data_dict.values():
                total_fields += 1
                if value and value != "" and value != "NO REGISTRADO":
                    filled_fields += 1
        
        enrichment["completeness_score"] = round((filled_fields / total_fields) * 100, 2) if total_fields > 0 else 0
        
        # 7. Record quality score (0-100)
        quality_score = 100
        
        # Deduct for missing data
        if not project_data:
            quality_score -= 40
        if not professional_data:
            quality_score -= 30
        
        # Deduct for errors/warnings
        quality_score -= len(record.get("validation", {}).get("errors", [])) * 10
        quality_score -= len(record.get("validation", {}).get("warnings", [])) * 2
        
        enrichment["quality_score"] = max(0, quality_score)
        
        self.stats["enrichments_added"] += 7  # Number of enrichment categories added
        
        logger.debug(f"Record {record_index}: Added enrichments, quality_score={enrichment['quality_score']}")
    
    # Utility methods
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text: uppercase, remove accents, trim"""
        if not text or pd.isna(text):
            return ""
        
        text = str(text).upper().strip()
        # Remove accents
        normalized = unicodedata.normalize('NFD', text)
        return normalized.encode('ascii', 'ignore').decode('ascii')
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Check if date string is in valid format"""
        if not date_str:
            return False
        
        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%d/%m/%Y %I:%M:%S %p"
        ]
        
        for fmt in formats:
            try:
                datetime.strptime(str(date_str), fmt)
                return True
            except (ValueError, TypeError):
                continue
        
        return False
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object"""
        formats = [
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d",
            "%d/%m/%Y %I:%M:%S %p"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except (ValueError, TypeError):
                continue
        
        return None
    
    def _is_valid_cedula(self, cedula: str) -> bool:
        """Validate Costa Rica cedula format"""
        if not cedula:
            return False
        
        # Remove hyphens/spaces
        cedula_clean = str(cedula).replace("-", "").replace(" ", "")
        
        # Should be 9-10 digits
        if not re.match(r'^\d{9,10}$', cedula_clean):
            return False
        
        return True
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format"""
        if not email:
            return False
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return self.stats.copy()


# Fix import for pd
import pandas as pd