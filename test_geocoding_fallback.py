"""
Test geocoding with fallback strategy
"""
import sys
import json
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from services.enhancement_service import EnhancementService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_geocoding_fallback():
    """Test geocoding with fallback on existing data"""

    workspace_dir = Path("workspaces/Ene-Mar25_20251114_235618_workdir")
    input_file = workspace_dir / "data/output/flattened.json"
    output_file = workspace_dir / "data/output/geocoded_test.json"

    if not input_file.exists():
        print(f"❌ Input file not found: {input_file}")
        return False

    print(f"✓ Input file found: {input_file}")
    print(f"✓ Output will be written to: {output_file}")

    # Create enhancement service
    service = EnhancementService()

    # Run geocoding with fallback
    print("\n🌍 Starting geocoding with fallback strategy...")
    result = service.add_geocoding(
        input_file=str(input_file),
        output_file=str(output_file),
        address_field="project_direccion_exacta",
        province_field="project_provincia",
        canton_field="project_canton",
        district_field="project_distrito",
        country="Costa Rica",
        rate_limit=1.0,
        context=None
    )

    print("\n📊 Results:")
    print(f"  Status: {result['status']}")
    print(f"  Total records: {result['stats']['total_records']}")
    print(f"  Newly geocoded: {result['stats']['geocoded']}")
    print(f"  From cache: {result['stats']['cached']}")
    print(f"  Failed: {result['stats']['failed']}")
    print(f"  Skipped: {result['stats']['skipped']}")
    print(f"\n  Level breakdown:")
    print(f"    Level 1 (Full address): {result['stats']['level_1']}")
    print(f"    Level 2 (District): {result['stats']['level_2']}")
    print(f"    Level 3 (Canton): {result['stats']['level_3']}")
    print(f"    Level 4 (Province): {result['stats']['level_4']}")

    # Calculate success rate
    total_attempted = result['stats']['total_records'] - result['stats']['skipped']
    total_succeeded = result['stats']['geocoded'] + result['stats']['cached']
    success_rate = (total_succeeded / total_attempted * 100) if total_attempted > 0 else 0

    print(f"\n  Success rate: {success_rate:.1f}% ({total_succeeded}/{total_attempted})")

    # Show some sample results
    print("\n📍 Sample geocoded records:")
    with open(output_file, 'r', encoding='utf-8') as f:
        records = json.load(f)

    sample_count = 0
    for record in records:
        if 'latitude' in record and sample_count < 5:
            sample_count += 1
            print(f"\n  Record {sample_count}:")
            print(f"    Address: {record.get('project_direccion_exacta', 'N/A')}")
            print(f"    Location: {record.get('project_distrito', 'N/A')}, {record.get('project_canton', 'N/A')}, {record.get('project_provincia', 'N/A')}")
            print(f"    Coordinates: ({record['latitude']}, {record['longitude']})")
            print(f"    Geocoding level: {record.get('geocoding_level', 'N/A')} - {record.get('geocoding_description', 'N/A')}")
            print(f"    Geocoded address: {record.get('geocoded_address', 'N/A')}")

    return True

if __name__ == "__main__":
    print("Testing geocoding with fallback strategy...\n")
    success = test_geocoding_fallback()

    if success:
        print("\n🎉 Geocoding test completed!")
    else:
        print("\n❌ Geocoding test failed!")
