"""
Test Suite for MergeService and ValidationEnrichmentService

Run this to verify the services work correctly
"""
import sys
import json
from pathlib import Path
from datetime import datetime
import tempfile

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))


def test_merge_service():
    """Test MergeService functionality"""
    print("="*80)
    print("TEST 1: MergeService")
    print("="*80)

    try:
        from services.merge_service import MergeService
        print("✓ MergeService imported successfully")

        service = MergeService()
        print("✓ MergeService instantiated")

        # Test statistics initialization
        assert service.stats["csv_rows_processed"] == 0
        assert service.stats["projects_matched"] == 0
        print("✓ Statistics initialized correctly")

        # Test _load_json_files with empty directory
        result = service._load_json_files("/nonexistent", "key")
        assert result == {}
        print("✓ Handles missing directory gracefully")

        print("\n✅ MergeService: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ MergeService test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_validation_service():
    """Test ValidationEnrichmentService functionality"""
    print("="*80)
    print("TEST 2: ValidationEnrichmentService")
    print("="*80)

    try:
        from services.validation_enrichment_service import ValidationEnrichmentService
        print("✓ ValidationEnrichmentService imported successfully")

        service = ValidationEnrichmentService()
        print("✓ ValidationEnrichmentService instantiated")

        # Test statistics initialization
        assert service.stats["records_processed"] == 0
        assert service.stats["records_valid"] == 0
        print("✓ Statistics initialized correctly")

        # Test normalize_text
        result = service._normalize_text("JOSÉ")
        assert result == "JOSE"
        print("✓ Text normalization works (accents removed)")

        result = service._normalize_text("  lower case  ")
        assert result == "LOWER CASE"
        print("✓ Text normalization works (uppercase, trimmed)")

        # Test date validation
        assert service._is_valid_date("2025-01-15") == True
        assert service._is_valid_date("15/01/2025") == True
        assert service._is_valid_date("invalid") == False
        print("✓ Date validation works")

        # Test cedula validation
        assert service._is_valid_cedula("0106980920") == True
        assert service._is_valid_cedula("1-234-567890") == True
        assert service._is_valid_cedula("invalid") == False
        print("✓ Cedula validation works")

        # Test email validation
        assert service._is_valid_email("test@example.com") == True
        assert service._is_valid_email("invalid-email") == False
        print("✓ Email validation works")

        # Test valid values
        assert "SAN JOSE" in service.VALID_PROVINCIAS
        assert "HABITACIONAL" in service.VALID_OBRAS
        print("✓ Valid categorical values defined")

        print("\n✅ ValidationEnrichmentService: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ ValidationEnrichmentService test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_merge_single_record():
    """Test merging a single record with mock data"""
    print("="*80)
    print("TEST 3: Merge Single Record")
    print("="*80)

    try:
        from services.merge_service import MergeService
        import pandas as pd

        service = MergeService()

        # Create mock CSV row
        csv_row = pd.Series({
            "id": "1196087-1",
            "proyecto": "1196087",
            "area": 36,
            "obra": "TURISTICO",
            "provincia": "ALAJUELA"
        })

        # Create mock project lookup
        projects_lookup = {
            "1196087": {
                "project_id": "1196087",
                "Estado": "Permiso de Construcción",
                "Carnet Profesional": "ICO-8244"
            }
        }

        # Create mock professional lookup
        professionals_lookup = {
            "ICO-8244": {
                "Carne": "ICO-8244",
                "NombreCompleto": "DANNY GONZALEZ",
                "Colegio": "COLEGIO DE INGENIEROS CIVILES"
            }
        }

        # Merge
        result = service._merge_single_row(
            csv_row,
            projects_lookup,
            professionals_lookup,
            0
        )

        # Verify structure
        assert "record_id" in result
        assert "csv_data" in result
        assert "project_data" in result
        assert "professional_data" in result
        assert "metadata" in result
        print("✓ Record structure correct")

        # Verify CSV data
        assert result["csv_data"]["id"] == "1196087-1"
        assert result["csv_data"]["proyecto"] == "1196087"
        print("✓ CSV data merged")

        # Verify project data
        assert result["project_data"]["project_id"] == "1196087"
        assert result["project_data"]["Estado"] == "Permiso de Construcción"
        print("✓ Project data merged")

        # Verify professional data
        assert result["professional_data"]["Carne"] == "ICO-8244"
        assert result["professional_data"]["NombreCompleto"] == "DANNY GONZALEZ"
        print("✓ Professional data merged")

        # Verify no warnings
        assert len(result["metadata"]["warnings"]) == 0
        print("✓ No warnings for complete data")

        print("\n✅ Merge Single Record: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Merge single record test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_validation_single_record():
    """Test validating and enriching a single record"""
    print("="*80)
    print("TEST 4: Validate and Enrich Single Record")
    print("="*80)

    try:
        from services.validation_enrichment_service import ValidationEnrichmentService

        service = ValidationEnrichmentService()

        # Create mock merged record
        record = {
            "record_id": "1196087-1",
            "csv_data": {
                "id": "1196087-1",
                "proyecto": "1196087",
                "area": 36,
                "obra": "TURISTICO",
                "subobra": "HOTEL",
                "provincia": "ALAJUELA",
                "canton": "SAN CARLOS",
                "distrito": "LA PALMERA",
                "exonerado": "NO"
            },
            "project_data": {
                "project_id": "1196087",
                "Estado": "Permiso de Construcción",
                "Tasado": "50000000.00",
                "Fecha Proyecto": "06/01/2025",
                "Provincia": "ALAJUELA"
            },
            "professional_data": {
                "Cedula": "0106980920",
                "Carne": "ICO-8244",
                "NombreCompleto": "DANNY GONZALEZ",
                "Colegio": "COLEGIO DE INGENIEROS CIVILES",
                "Lugar": "Company Name"
            },
            "metadata": {
                "merged_at": datetime.now().isoformat(),
                "warnings": []
            }
        }

        # Validate and enrich
        result = service._validate_and_enrich_record(record, {}, 0)

        # Verify validation section added
        assert "validation" in result
        assert "is_valid" in result["validation"]
        assert "errors" in result["validation"]
        assert "warnings" in result["validation"]
        print("✓ Validation section added")

        # Verify enrichment section added
        assert "enrichment" in result
        print("✓ Enrichment section added")

        # Verify specific enrichments
        assert "location_normalized" in result["enrichment"]
        assert result["enrichment"]["location_normalized"]["provincia"] == "ALAJUELA"
        print("✓ Location normalized")

        assert "classification" in result["enrichment"]
        assert result["enrichment"]["classification"]["is_residential"] == False
        assert result["enrichment"]["classification"]["is_exonerated"] == False
        print("✓ Classification metadata added")

        assert "financial" in result["enrichment"]
        assert result["enrichment"]["financial"]["tasado_amount"] == 50000000.0
        print("✓ Financial analysis added")

        assert "professional_info" in result["enrichment"]
        assert result["enrichment"]["professional_info"]["is_engineer"] == True
        assert result["enrichment"]["professional_info"]["has_company"] == True
        print("✓ Professional metadata added")

        assert "completeness_score" in result["enrichment"]
        assert result["enrichment"]["completeness_score"] > 0
        print("✓ Completeness score calculated")

        assert "quality_score" in result["enrichment"]
        assert 0 <= result["enrichment"]["quality_score"] <= 100
        print(f"✓ Quality score: {result['enrichment']['quality_score']}")

        print("\n✅ Validate and Enrich Single Record: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Validate/enrich single record test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_validation_flattened_record():
    """Test validating and enriching a flattened pipeline record"""
    print("="*80)
    print("TEST 5: Validate and Enrich Flattened Record")
    print("="*80)

    try:
        from services.validation_enrichment_service import ValidationEnrichmentService

        service = ValidationEnrichmentService()

        record = {
            "record_id": "1270797-1",
            "csv_id": "1270797-1",
            "csv_proyecto": 1270797,
            "csv_exonerado": "SI",
            "csv_area": 42.0,
            "csv_obra": "HABITACIONAL",
            "csv_subobra": "CASA INTERES SOCIAL-EXONERADA",
            "csv_fechaproyecto": "2025-12-23",
            "csv_provincia": "ALAJUELA",
            "csv_canton": "SAN CARLOS",
            "csv_distrito": "MONTERREY",
            "project_project_id": "1270797",
            "project_fecha_proyecto": "2025-12-23",
            "project_estado": "PENDIENTE DE PAGO",
            "project_tasado": "9393000",
            "project_provincia": "ALAJUELA",
            "project_canton": "SAN CARLOS",
            "project_distrito": "MONTERREY",
            "professional_cedula": "106390433",
            "professional_carne": "ICO-5878",
            "professional_nombrecompleto": "SERGIO GAIRAUD BONILLA",
            "professional_colegio": "COLEGIO DE INGENIEROS TECNOLOGOS",
            "professional_lugar": "INDEPENDIENTE",
            "validation": {
                "is_valid": False,
                "errors": [
                    "Missing required field: proyecto",
                    "Missing required field: id"
                ],
                "warnings": []
            },
            "enrichment": {
                "location_normalized": {
                    "provincia": "",
                    "canton": "",
                    "distrito": "",
                    "full_location": None
                },
                "classification": {
                    "category": "",
                    "subcategory": "",
                    "is_residential": False,
                    "is_social_interest": False,
                    "is_exonerated": False
                },
                "completeness_score": 0,
                "quality_score": 10
            }
        }

        result = service._validate_and_enrich_record(record, {}, 0)

        assert result["validation"]["errors"] == []
        assert result["validation"]["is_valid"] == True
        print("✓ Stale validation errors cleared")

        location = result["enrichment"]["location_normalized"]
        assert location["provincia"] == "ALAJUELA"
        assert location["canton"] == "SAN CARLOS"
        assert location["distrito"] == "MONTERREY"
        print("✓ Flat location normalized")

        classification = result["enrichment"]["classification"]
        assert classification["category"] == "HABITACIONAL"
        assert classification["subcategory"] == "CASA INTERES SOCIAL-EXONERADA"
        assert classification["is_residential"] == True
        assert classification["is_social_interest"] == True
        assert classification["is_exonerated"] == True
        print("✓ Flat classification metadata added")

        assert result["enrichment"]["financial"]["tasado_amount"] == 9393000.0
        assert result["enrichment"]["financial"]["price_per_m2"] == 223642.86
        print("✓ Flat financial metadata added")

        professional = result["enrichment"]["professional_info"]
        assert professional["college"] == "COLEGIO DE INGENIEROS TECNOLOGOS"
        assert professional["license_prefix"] == "ICO"
        assert professional["is_engineer"] == True
        print("✓ Flat professional metadata added")

        assert result["enrichment"]["completeness_score"] > 0
        assert result["enrichment"]["quality_score"] > 10
        print("✓ Quality scores recalculated")

        print("\n✅ Validate and Enrich Flattened Record: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Validate/enrich flattened record test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_deterministic_project_summary():
    """Test deterministic summary includes exact professional search tokens"""
    print("="*80)
    print("TEST 6: Deterministic Project Summary")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        record = {
            "record_id": "1270797-1",
            "csv_proyecto": 1270797,
            "csv_area": 42.0,
            "csv_unidad": "M2",
            "csv_obra": "HABITACIONAL",
            "csv_subobra": "CASA INTERES SOCIAL-EXONERADA",
            "project_descripcion_del_proyecto": "CASA DE HABITACION MARISOL FERNANDEZ GUADAMUZ",
            "project_clasificacion": "VIVIENDA",
            "project_direccion_exacta": "MONTERREY, 300 O Y 260 S DE LA CLINICA",
            "project_provincia": "ALAJUELA",
            "project_canton": "SAN CARLOS",
            "project_distrito": "MONTERREY",
            "project_tasado": "9393000",
            "project_estado": "PENDIENTE DE PAGO",
            "professional_cedula": "106390433",
            "professional_carne": "ICO-5878",
            "professional_nombrecompleto": "SERGIO GAIRAUD BONILLA",
            "professional_colegio": "COLEGIO DE INGENIEROS TECNOLOGOS",
        }

        summary = service.build_project_search_summary(record)

        assert "SERGIO GAIRAUD BONILLA" in summary
        assert "106390433" in summary
        assert "ICO-5878" in summary
        assert "[Nombre del profesional]" not in summary
        assert "[Número de identificación y carné]" not in summary
        print("✓ Summary includes exact professional name, cedula, and carne")

        print("\n✅ Deterministic Project Summary: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Deterministic project summary test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_geopoint_location_field():
    """Test OpenSearch geo_point location is created from coordinates"""
    print("="*80)
    print("TEST 7: Geo Point Location Field")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        record = {
            "latitude": "10.5761473",
            "longitude": "-84.6319195"
        }

        assert service.apply_location_field(record) == True
        assert record["location"] == {
            "lat": 10.5761473,
            "lon": -84.6319195
        }

        bad_record = {"latitude": "999", "longitude": "-84.0"}
        assert service.apply_location_field(bad_record) == False
        assert "location" not in bad_record
        print("✓ Geo point location field created and invalid coordinates rejected")

        print("\n✅ Geo Point Location Field: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Geo point location field test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_prepare_for_indexing_service():
    """Test search-ready preparation adds index contract fields"""
    print("="*80)
    print("TEST 8: Prepare For Indexing Service")
    print("="*80)

    try:
        from services.search_preparation_service import SearchPreparationService

        service = SearchPreparationService()
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "search_ready.json"
            input_file.write_text(json.dumps([{
                "record_id": "1270797-1",
                "latitude": 10.5761473,
                "longitude": -84.6319195,
                "csv_area": 42.0,
                "csv_proyecto": 1270797,
                "project_tasado": "9393000",
                "resumen": "Profesional responsable: SERGIO GAIRAUD BONILLA. Carne profesional: ICO-5878.",
                "embedding": [0.1, 0.2, 0.3],
            }]), encoding="utf-8")

            result = service.prepare_for_indexing(
                input_file=str(input_file),
                output_file=str(output_file),
                expected_embedding_dim=3,
                progress_interval=0,
            )
            record = json.loads(output_file.read_text(encoding="utf-8"))[0]

        assert result["stats"]["ready"] == 1
        assert result["stats"]["not_ready"] == 0
        assert record["index_ready"]["is_ready"] == True
        assert record["location"] == {"lat": 10.5761473, "lon": -84.6319195}
        assert record["embedding_dimension"] == 3
        assert record["project_tasado_num"] == 9393000
        assert record["csv_area_num"] == 42.0
        assert record["price_per_m2_num"] == 223642.86
        print("✓ Search-ready index fields added")

        print("\n✅ Prepare For Indexing Service: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Prepare for indexing service test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_opensearch_service_streaming_load():
    """Test OpenSearch load streams JSON, batches records, and skips not-ready docs"""
    print("="*80)
    print("TEST 9: OpenSearch Service Streaming Load")
    print("="*80)

    try:
        from services.opensearch_service import OpenSearchService

        class FakeLoader:
            def __init__(self):
                self.created = []
                self.batches = []

            def create_index(self, index_name, mappings=None, settings=None):
                self.created.append((index_name, mappings, settings))
                return True

            def bulk_index(self, index_name, documents, id_field=None, chunk_size=500):
                docs = list(documents)
                self.batches.append({
                    "index_name": index_name,
                    "documents": docs,
                    "id_field": id_field,
                    "chunk_size": chunk_size,
                })
                return {"success": True, "indexed": len(docs), "failed": 0}

        service = OpenSearchService()
        fake_loader = FakeLoader()
        service._get_loader = lambda **kwargs: fake_loader

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "search_ready.json"
            input_file.write_text(json.dumps([
                {"record_id": "1", "index_ready": {"is_ready": True}},
                {"record_id": "2", "index_ready": {"is_ready": False}},
                {"record_id": "3", "index_ready": {"is_ready": True}},
            ]), encoding="utf-8")

            result = service.load_data(
                input_file=str(input_file),
                index_name="asidelco-explorer-test",
                batch_size=1,
                id_field="record_id",
                mappings={"properties": {}},
                settings={"index": {"number_of_replicas": 0}},
                progress_interval=0,
            )

        assert fake_loader.created == [(
            "asidelco-explorer-test",
            {"properties": {}},
            {"index": {"number_of_replicas": 0}},
        )]
        assert result["count"] == 3
        assert result["indexed"] == 2
        assert result["failed"] == 0
        assert result["skipped_not_ready"] == 1
        assert len(fake_loader.batches) == 2
        assert [batch["documents"][0]["record_id"] for batch in fake_loader.batches] == ["1", "3"]
        assert all(batch["id_field"] == "record_id" for batch in fake_loader.batches)
        print("✓ Streams JSON array, skips not-ready records, and batches correctly")

        print("\n✅ OpenSearch Service Streaming Load: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ OpenSearch streaming load test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_generate_summaries_replaces_placeholder():
    """Test future pipeline summaries replace bad placeholder summaries"""
    print("="*80)
    print("TEST 10: Generate Summaries Replaces Placeholder")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "output.json"
            input_file.write_text(json.dumps([{
                "record_id": "1270797-1",
                "csv_proyecto": 1270797,
                "project_descripcion_del_proyecto": "CASA DE HABITACION",
                "professional_nombrecompleto": "SERGIO GAIRAUD BONILLA",
                "professional_cedula": "106390433",
                "professional_carne": "ICO-5878",
                "resumen": "Profesional responsable: [Nombre del profesional]."
            }]), encoding="utf-8")

            service.generate_summaries(
                input_file=str(input_file),
                output_file=str(output_file),
                skip_existing=True,
                use_ai=False
            )
            result = json.loads(output_file.read_text(encoding="utf-8"))[0]

        assert "SERGIO GAIRAUD BONILLA" in result["resumen"]
        assert "106390433" in result["resumen"]
        assert "ICO-5878" in result["resumen"]
        assert "[Nombre del profesional]" not in result["resumen"]
        assert result["summary_model"] == "deterministic-search-summary-v1"
        print("✓ Placeholder summary replaced deterministically")

        print("\n✅ Generate Summaries Replaces Placeholder: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Generate summaries placeholder replacement test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_embedding_force_regenerate_flag():
    """Test embedding generation honors force_regenerate for existing embeddings"""
    print("="*80)
    print("TEST 10: Embedding Force Regenerate Flag")
    print("="*80)

    try:
        from services.embedding_service import EmbeddingService

        service = EmbeddingService.__new__(EmbeddingService)
        service.model = "test-embedding-model"
        service.generate_embedding = lambda text: [float(len(text))]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            skip_output = Path(tmpdir) / "skip.json"
            force_output = Path(tmpdir) / "force.json"
            input_file.write_text(json.dumps([{
                "record_id": "1",
                "resumen": "new corrected summary",
                "embedding": [999.0],
                "embedding_model": "old-model"
            }]), encoding="utf-8")

            skipped = service.generate_embeddings(
                input_file=str(input_file),
                output_file=str(skip_output),
                text_field="resumen",
                force_regenerate=False
            )
            forced = service.generate_embeddings(
                input_file=str(input_file),
                output_file=str(force_output),
                text_field="resumen",
                force_regenerate=True
            )
            forced_record = json.loads(force_output.read_text(encoding="utf-8"))[0]

        assert skipped["skipped"] == 1
        assert forced["new_embeddings"] == 1
        assert forced_record["embedding"] == [21.0]
        assert forced_record["embedding_model"] == "test-embedding-model"
        print("✓ Existing embeddings can be forcibly regenerated")

        print("\n✅ Embedding Force Regenerate Flag: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Embedding force regenerate test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_geocoding_cache_ignores_negative_entries():
    """Test failed geocodes are not permanently reused from cache"""
    print("="*80)
    print("TEST 11: Geocoding Cache Ignores Negative Entries")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "geocode_cache.json"
            cache_file.write_text(json.dumps({
                "CARTAGO, Costa Rica": None,
                "ALAJUELA, Costa Rica": {
                    "latitude": 10.0162,
                    "longitude": -84.2116
                }
            }), encoding="utf-8")

            service._load_geocode_cache(cache_file)
            service._save_geocode_cache(cache_file)
            saved = json.loads(cache_file.read_text(encoding="utf-8"))

        assert "CARTAGO, Costa Rica" not in service.geocode_cache
        assert "CARTAGO, Costa Rica" not in saved
        assert service.geocode_cache["ALAJUELA, Costa Rica"]["latitude"] == 10.0162
        print("✓ Stale None geocode cache entries are ignored and removed")

        print("\n✅ Geocoding Cache Ignores Negative Entries: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Geocoding cache negative-entry test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_geocoding_csv_fallback_and_local_centroid():
    """Test geocoding can recover CSV-only locations with local fallback"""
    print("="*80)
    print("TEST 12: Geocoding CSV Fallback And Local Centroid")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        service._geocode_address = (
            lambda address, max_retries=3, allow_external=True, external_rate_limit=0.0: None
        )
        record = {
            "project_provincia": "",
            "csv_provincia": "Cartago",
            "csv_canton": "El Guarco",
            "csv_distrito": "Tejar",
        }

        assert service._record_geo_value(record, "project_provincia") == "Cartago"
        result = service._geocode_with_fallback(
            street=None,
            district=service._record_geo_value(record, "project_distrito"),
            canton=service._record_geo_value(record, "project_canton"),
            province=service._record_geo_value(record, "project_provincia"),
            country="Costa Rica",
            max_retries=1,
        )

        assert result["geocoding_level"] == 5
        assert result["geocoding_source"] == "local_admin_centroid"
        assert result["geocoding_precision"] == "province"
        record["latitude"] = result["latitude"]
        record["longitude"] = result["longitude"]
        assert service.apply_location_field(record) == True
        print("✓ CSV-only location fields recover to local province centroid")

        print("\n✅ Geocoding CSV Fallback And Local Centroid: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Geocoding CSV fallback/local-centroid test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_repair_geocoding_uses_dataset_centroids():
    """Test local province centroids are improved from dataset coordinates"""
    print("="*80)
    print("TEST 13: Repair Geocoding Uses Dataset Centroids")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        records = [
            {
                "record_id": "source-district",
                "project_provincia": "SAN JOSE",
                "project_canton": "SAN JOSE",
                "project_distrito": "ZAPOTE",
                "latitude": 9.92,
                "longitude": -84.05,
                "location": {"lat": 9.92, "lon": -84.05},
                "geocoding_source": "nominatim",
            },
            {
                "record_id": "repair-district",
                "project_provincia": "SAN JOSE",
                "project_canton": "SAN JOSE",
                "project_distrito": "ZAPOTE",
                "latitude": 9.9281,
                "longitude": -84.0907,
                "location": {"lat": 9.9281, "lon": -84.0907},
                "geocoding_source": "local_admin_centroid",
            },
            {
                "record_id": "source-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "POTRERO CERRADO",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "nominatim",
            },
            {
                "record_id": "repair-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "DISTRICT WITHOUT MANUAL COORDINATES",
                "latitude": 9.8644,
                "longitude": -83.9194,
                "location": {"lat": 9.8644, "lon": -83.9194},
                "geocoding_source": "local_admin_centroid",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "output.json"
            input_file.write_text(json.dumps(records), encoding="utf-8")

            result = service.repair_missing_geocoding(
                input_file=str(input_file),
                output_file=str(output_file),
                allow_external=False,
                progress_interval=0,
            )
            output = {
                record["record_id"]: record
                for record in json.loads(output_file.read_text(encoding="utf-8"))
            }

        district = output["repair-district"]
        canton = output["repair-canton"]

        assert result["stats"]["local_admin_centroid_repaired"] == 2
        assert result["stats"]["approximate_centroid_repaired"] == 2
        assert result["stats"]["dataset_district_centroid"] == 1
        assert result["stats"]["dataset_canton_centroid"] == 1
        assert district["geocoding_precision"] == "district_derived"
        assert district["geocoding_source"] == "dataset_admin_centroid"
        assert district["location"] == {"lat": 9.92, "lon": -84.05}
        assert canton["geocoding_precision"] == "canton_derived"
        assert canton["geocoding_source"] == "dataset_admin_centroid"
        assert canton["location"] == {"lat": 9.98, "lon": -83.86}
        print("✓ Province centroid records improved from dataset district/canton coordinates")

        print("\n✅ Repair Geocoding Uses Dataset Centroids: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Dataset centroid geocoding repair test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_repair_geocoding_uses_cache_before_dataset_centroid():
    """Test geocoding repair tries cache before dataset fallback"""
    print("="*80)
    print("TEST 14: Repair Geocoding Uses Cache Before Dataset Centroid")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        records = [
            {
                "record_id": "source-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "POTRERO CERRADO",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "nominatim",
            },
            {
                "record_id": "repair-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "CIPRESES",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "dataset_admin_centroid",
                "geocoding_precision": "canton_derived",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "output.json"
            cache_file = Path(tmpdir) / "geocode_cache.json"
            input_file.write_text(json.dumps(records), encoding="utf-8")
            cache_file.write_text(json.dumps({
                "CIPRESES, OREAMUNO, CARTAGO, Costa Rica": {
                    "latitude": 9.935,
                    "longitude": -83.82,
                }
            }), encoding="utf-8")

            result = service.repair_missing_geocoding(
                input_file=str(input_file),
                output_file=str(output_file),
                allow_external=False,
                progress_interval=0,
            )
            output = {
                record["record_id"]: record
                for record in json.loads(output_file.read_text(encoding="utf-8"))
            }

        repaired = output["repair-canton"]

        assert result["stats"]["cached"] == 1
        assert result["stats"]["dataset_canton_centroid"] == 0
        assert repaired["location"] == {"lat": 9.935, "lon": -83.82}
        assert repaired["geocoding_source"] == "nominatim"
        assert repaired["geocoding_precision"] == "district"
        print("✓ Cached district geocode wins before dataset centroid fallback")

        print("\n✅ Repair Geocoding Uses Cache Before Dataset Centroid: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Cache-before-dataset geocoding repair test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_repair_geocoding_uses_online_before_dataset_centroid():
    """Test geocoding repair tries online geocoder before dataset fallback"""
    print("="*80)
    print("TEST 15: Repair Geocoding Uses Online Before Dataset Centroid")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        class FakeLocation:
            latitude = 9.936
            longitude = -83.821

        class FakeGeocoder:
            def geocode(self, address, timeout=10):
                return FakeLocation()

        service = EnhancementService()
        service.geocoder = FakeGeocoder()
        records = [
            {
                "record_id": "source-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "POTRERO CERRADO",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "nominatim",
            },
            {
                "record_id": "repair-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "CIPRESES",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "dataset_admin_centroid",
                "geocoding_precision": "canton_derived",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "output.json"
            cache_file = Path(tmpdir) / "geocode_cache.json"
            input_file.write_text(json.dumps(records), encoding="utf-8")

            result = service.repair_missing_geocoding(
                input_file=str(input_file),
                output_file=str(output_file),
                allow_external=True,
                rate_limit=0,
                progress_interval=0,
            )
            output = {
                record["record_id"]: record
                for record in json.loads(output_file.read_text(encoding="utf-8"))
            }
            cache = json.loads(cache_file.read_text(encoding="utf-8"))

        repaired = output["repair-canton"]

        assert result["stats"]["online_geocoded"] == 1
        assert result["stats"]["dataset_canton_centroid"] == 0
        assert repaired["location"] == {"lat": 9.936, "lon": -83.821}
        assert repaired["geocoding_source"] == "nominatim"
        assert repaired["geocoding_precision"] == "district"
        assert "CIPRESES, OREAMUNO, CARTAGO, Costa Rica" in cache
        print("✓ Online district geocode wins before dataset centroid fallback and is cached")

        print("\n✅ Repair Geocoding Uses Online Before Dataset Centroid: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Online-before-dataset geocoding repair test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_repair_geocoding_uses_manual_before_dataset_centroid():
    """Test known manual district coordinates win before dataset fallback"""
    print("="*80)
    print("TEST 16: Repair Geocoding Uses Manual Before Dataset Centroid")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        records = [
            {
                "record_id": "source-canton",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "POTRERO CERRADO",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "nominatim",
            },
            {
                "record_id": "repair-manual",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "CIPRESES",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "dataset_admin_centroid",
                "geocoding_precision": "canton_derived",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "output.json"
            input_file.write_text(json.dumps(records), encoding="utf-8")

            result = service.repair_missing_geocoding(
                input_file=str(input_file),
                output_file=str(output_file),
                allow_external=False,
                progress_interval=0,
            )
            output = {
                record["record_id"]: record
                for record in json.loads(output_file.read_text(encoding="utf-8"))
            }

        repaired = output["repair-manual"]

        assert result["stats"]["manual_district_centroid"] == 1
        assert result["stats"]["dataset_canton_centroid"] == 0
        assert repaired["geocoding_source"] == "manual_district_centroid"
        assert repaired["geocoding_precision"] == "district_manual"
        assert repaired["location"] == {"lat": 9.8938889, "lon": -83.8419444}
        print("✓ Manual district centroid wins before dataset centroid fallback")

        print("\n✅ Repair Geocoding Uses Manual Before Dataset Centroid: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Manual-before-dataset geocoding repair test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_repair_geocoding_does_not_reuse_dataset_centroid_as_source():
    """Test existing dataset centroids are not reused as exact centroid sources"""
    print("="*80)
    print("TEST 17: Repair Geocoding Does Not Reuse Dataset Centroid As Source")
    print("="*80)

    try:
        from services.enhancement_service import EnhancementService

        service = EnhancementService()
        records = [
            {
                "record_id": "source-approximate",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "DISTRICT WITHOUT MANUAL COORDINATES",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "dataset_admin_centroid",
                "geocoding_precision": "canton_derived",
            },
            {
                "record_id": "repair-approximate",
                "project_provincia": "CARTAGO",
                "project_canton": "OREAMUNO",
                "project_distrito": "DISTRICT WITHOUT MANUAL COORDINATES",
                "latitude": 9.98,
                "longitude": -83.86,
                "location": {"lat": 9.98, "lon": -83.86},
                "geocoding_source": "dataset_admin_centroid",
                "geocoding_precision": "canton_derived",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.json"
            output_file = Path(tmpdir) / "output.json"
            input_file.write_text(json.dumps(records), encoding="utf-8")

            result = service.repair_missing_geocoding(
                input_file=str(input_file),
                output_file=str(output_file),
                allow_external=False,
                progress_interval=0,
            )
            output = json.loads(output_file.read_text(encoding="utf-8"))

        assert result["stats"]["dataset_district_centroid"] == 0
        assert result["stats"]["dataset_canton_centroid"] == 0
        assert all(record["geocoding_source"] == "local_admin_centroid" for record in output)
        print("✓ Existing dataset centroids are not reused as source evidence")

        print("\n✅ Repair Geocoding Does Not Reuse Dataset Centroid As Source: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Dataset-centroid source exclusion test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_edge_cases():
    """Test edge cases and error handling"""
    print("="*80)
    print("TEST 5: Edge Cases")
    print("="*80)

    try:
        from services.merge_service import MergeService
        import pandas as pd

        service = MergeService()

        # Test 1: Missing proyecto in CSV
        csv_row = pd.Series({"id": "test-1"})
        result = service._merge_single_row(csv_row, {}, {}, 0)
        assert len(result["metadata"]["warnings"]) > 0
        print("✓ Handles missing proyecto")

        # Test 2: Project not found in lookup
        csv_row = pd.Series({"id": "test-1", "proyecto": "999999"})
        result = service._merge_single_row(csv_row, {}, {}, 0)
        assert result["project_data"] == {}
        assert any("not found" in w.lower() for w in result["metadata"]["warnings"])
        print("✓ Handles missing project gracefully")

        # Test 3: Multiple carnets (comma-separated)
        projects_lookup = {
            "1196087": {
                "project_id": "1196087",
                "Carnet Profesional": "ICO-8244, ICO-9999"
            }
        }
        professionals_lookup = {
            "ICO-8244": {"Carne": "ICO-8244", "NombreCompleto": "Test"}
        }

        csv_row = pd.Series({"id": "1196087-1", "proyecto": "1196087"})
        result = service._merge_single_row(csv_row, projects_lookup, professionals_lookup, 0)
        assert result["professional_data"]["Carne"] == "ICO-8244"
        assert any("Multiple carnets" in w for w in result["metadata"]["warnings"])
        print("✓ Handles multiple carnets (uses first)")

        print("\n✅ Edge Cases: All tests passed\n")
        return True

    except Exception as e:
        print(f"\n❌ Edge cases test failed: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests and report results"""
    print("\n" + "="*80)
    print("RUNNING TEST SUITE")
    print("="*80 + "\n")

    tests = [
        ("MergeService", test_merge_service),
        ("ValidationEnrichmentService", test_validation_service),
        ("Merge Single Record", test_merge_single_record),
        ("Validate/Enrich Single Record", test_validation_single_record),
        ("Validate/Enrich Flattened Record", test_validation_flattened_record),
        ("Deterministic Project Summary", test_deterministic_project_summary),
        ("Geo Point Location Field", test_geopoint_location_field),
        ("Prepare For Indexing Service", test_prepare_for_indexing_service),
        ("OpenSearch Service Streaming Load", test_opensearch_service_streaming_load),
        ("Generate Summaries Replaces Placeholder", test_generate_summaries_replaces_placeholder),
        ("Embedding Force Regenerate Flag", test_embedding_force_regenerate_flag),
        ("Geocoding Cache Ignores Negative Entries", test_geocoding_cache_ignores_negative_entries),
        ("Geocoding CSV Fallback And Local Centroid", test_geocoding_csv_fallback_and_local_centroid),
        ("Repair Geocoding Uses Dataset Centroids", test_repair_geocoding_uses_dataset_centroids),
        ("Repair Geocoding Uses Cache Before Dataset Centroid", test_repair_geocoding_uses_cache_before_dataset_centroid),
        ("Repair Geocoding Uses Online Before Dataset Centroid", test_repair_geocoding_uses_online_before_dataset_centroid),
        ("Repair Geocoding Uses Manual Before Dataset Centroid", test_repair_geocoding_uses_manual_before_dataset_centroid),
        ("Repair Geocoding Does Not Reuse Dataset Centroid As Source", test_repair_geocoding_does_not_reuse_dataset_centroid_as_source),
        ("Edge Cases", test_edge_cases)
    ]

    results = []

    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"❌ Test '{name}' crashed: {e}")
            results.append((name, False))

    # Summary
    print("="*80)
    print("TEST SUMMARY")
    print("="*80)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {name}")

    print("="*80)
    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Services are working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Review errors above.")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
