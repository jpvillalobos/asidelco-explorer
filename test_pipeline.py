"""
Simple test to verify pipeline setup
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_imports():
    """Test that all imports work"""
    try:
        from src.pipeline.steps import StepType
        print("✓ StepType imported successfully")
        
        from src.pipeline.progress import ProgressTracker
        print("✓ ProgressTracker imported successfully")
        
        from src.pipeline.registry import StepRegistry
        print("✓ StepRegistry imported successfully")
        
        from src.pipeline.pipeline import Pipeline
        print("✓ Pipeline imported successfully")
        
        # Test pipeline creation
        pipeline = Pipeline()
        print("✓ Pipeline created successfully")
        
        # Test registry
        steps = pipeline.registry.list_steps()
        print(f"✓ Found {len(steps)} registered steps")
        
        for step_type, config in steps.items():
            print(f"  - {step_type.value}: {config.name}")
        
        return True
        
    except Exception as e:
        print(f"✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing pipeline imports...")
    success = test_imports()
    
    if success:
        print("\n🎉 All imports successful! Streamlit should work now.")
    else:
        print("\n❌ There are still import issues to fix.")