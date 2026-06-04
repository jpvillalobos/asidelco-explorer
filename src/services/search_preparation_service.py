"""
Search Index Preparation Service
"""
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import json
import logging
import re

from services.enhancement_service import EnhancementService
from services.validation_enrichment_service import ValidationEnrichmentService

logger = logging.getLogger(__name__)


class SearchPreparationService:
    """Prepare enhanced records for OpenSearch indexing."""

    BAD_SUMMARY_MARKERS = [
        "[Nombre del profesional]",
        "[Número de identificación y carné]",
        "[Numero de identificacion y carne]",
        "[Número de identificación",
        "[Numero de identificacion",
    ]

    def __init__(self):
        self.enhancement_service = EnhancementService()
        self.validation_service = ValidationEnrichmentService()

    def prepare_for_indexing(
        self,
        input_file: str,
        output_file: str,
        embedding_field: str = "embedding",
        expected_embedding_dim: int = 1536,
        summary_field: str = "resumen",
        chunk_size: int = 1024 * 1024,
        progress_interval: int = 10000,
        context: Optional[object] = None,
    ) -> Dict[str, Any]:
        """
        Prepare a JSON array for search indexing without loading it all in memory.

        The output keeps all original fields and adds index-oriented metadata and
        convenience fields used by OpenSearch dashboards/search:
        - location: geo_point object from latitude/longitude
        - *_num numeric fields for common range filters
        - embedding_dimension
        - index_ready metadata
        """
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        stats = {
            "total_records": 0,
            "ready": 0,
            "not_ready": 0,
            "location_added": 0,
            "missing_location": 0,
            "missing_summary": 0,
            "placeholder_summary": 0,
            "missing_embedding": 0,
            "bad_embedding_dimension": 0,
            "numeric_fields_added": 0,
        }

        logger.info("Starting search index preparation")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Expected embedding dim: {expected_embedding_dim}")

        with open(output_path, "w", encoding="utf-8") as out:
            out.write("[\n")
            first_record = True

            for idx, record in enumerate(
                self.validation_service._iter_json_array(input_path, chunk_size=chunk_size)
            ):
                prepared = self._prepare_record(
                    record=record,
                    embedding_field=embedding_field,
                    expected_embedding_dim=expected_embedding_dim,
                    summary_field=summary_field,
                    stats=stats,
                )

                if not first_record:
                    out.write(",\n")
                json.dump(prepared, out, ensure_ascii=False)
                first_record = False

                stats["total_records"] += 1
                if context and stats["total_records"] % progress_interval == 0:
                    context.report_progress(
                        stats["total_records"],
                        0,
                        f"Prepared {stats['total_records']} records",
                        stats.copy(),
                    )
                elif progress_interval and stats["total_records"] % progress_interval == 0:
                    logger.info(
                        "Prepared %s records (%s ready, %s not ready)",
                        stats["total_records"],
                        stats["ready"],
                        stats["not_ready"],
                    )

            out.write("\n]\n")

        logger.info("Search index preparation completed")
        logger.info(f"  Records: {stats['total_records']}")
        logger.info(f"  Ready: {stats['ready']}")
        logger.info(f"  Not ready: {stats['not_ready']}")

        return {
            "status": "success",
            "output_file": str(output_path),
            "count": stats["total_records"],
            "stats": stats,
        }

    def _prepare_record(
        self,
        record: Dict[str, Any],
        embedding_field: str,
        expected_embedding_dim: int,
        summary_field: str,
        stats: Dict[str, int],
    ) -> Dict[str, Any]:
        prepared = record.copy()
        errors = []
        warnings = []

        if self.enhancement_service.apply_location_field(prepared):
            stats["location_added"] += 1
        else:
            prepared.pop("location", None)
            stats["missing_location"] += 1
            warnings.append("Missing valid location geo_point")

        numeric_added = self._add_numeric_fields(prepared)
        stats["numeric_fields_added"] += numeric_added

        summary = str(prepared.get(summary_field) or "")
        if not summary.strip():
            stats["missing_summary"] += 1
            errors.append(f"Missing summary field: {summary_field}")
        elif self._has_bad_summary_placeholder(summary):
            stats["placeholder_summary"] += 1
            errors.append("Summary contains unresolved placeholder")

        embedding = prepared.get(embedding_field)
        if not embedding:
            stats["missing_embedding"] += 1
            errors.append(f"Missing embedding field: {embedding_field}")
            prepared["embedding_dimension"] = 0
        elif not isinstance(embedding, list):
            stats["bad_embedding_dimension"] += 1
            errors.append(f"Embedding field is not a list: {embedding_field}")
            prepared["embedding_dimension"] = 0
        else:
            prepared["embedding_dimension"] = len(embedding)
            if len(embedding) != expected_embedding_dim:
                stats["bad_embedding_dimension"] += 1
                errors.append(
                    f"Embedding dimension {len(embedding)} != expected {expected_embedding_dim}"
                )

        prepared["index_ready"] = {
            "is_ready": not errors,
            "errors": errors,
            "warnings": warnings,
            "prepared_at": datetime.now().isoformat(),
        }

        if errors:
            stats["not_ready"] += 1
        else:
            stats["ready"] += 1

        return prepared

    def _add_numeric_fields(self, record: Dict[str, Any]) -> int:
        added = 0
        numeric_fields = {
            "project_tasado": "project_tasado_num",
            "csv_area": "csv_area_num",
            "csv_proyecto": "csv_proyecto_num",
            "project_monto_pendiente_de_pago": "project_monto_pendiente_de_pago_num",
            "project_cantidad_de_dias_en_cola_de_revision_de_la_municipalidad": (
                "project_dias_cola_municipalidad_num"
            ),
        }

        for source_field, target_field in numeric_fields.items():
            value = self._to_number(record.get(source_field))
            if value is None:
                continue
            record[target_field] = value
            added += 1

        tasado = record.get("project_tasado_num")
        area = record.get("csv_area_num")
        if isinstance(tasado, (int, float)) and isinstance(area, (int, float)) and area > 0:
            record["price_per_m2_num"] = round(tasado / area, 2)
            added += 1

        return added

    def _to_number(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None

        if isinstance(value, (int, float)):
            return value

        if not isinstance(value, str):
            return None

        cleaned = value.strip().replace(",", "")
        if not cleaned:
            return None

        try:
            number = float(cleaned)
        except ValueError:
            return None

        return int(number) if number.is_integer() else number

    def _has_bad_summary_placeholder(self, summary: str) -> bool:
        if any(marker in summary for marker in self.BAD_SUMMARY_MARKERS):
            return True

        # Catch unresolved bracketed template remnants but allow real descriptions
        # like "VIVIENDA [LAURA PINEDA]".
        unresolved = re.search(r"\[(?:Nombre|N[uú]mero|Numero|identificaci[oó]n)", summary, re.I)
        return bool(unresolved)
