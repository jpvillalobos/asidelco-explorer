"""
Test Suite for MergeService and ValidationEnrichmentService

Run this to verify the services work correctly
"""
import sys
import json
from pathlib import Path
from datetime import datetime

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