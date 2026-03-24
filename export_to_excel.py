#!/usr/bin/env python3
"""
Quick script to export JSON data to Excel
Usage: python export_to_excel.py <input.json> <output.xlsx>
"""
import sys
import pandas as pd
import json
from pathlib import Path

def export_json_to_excel(input_file, output_file=None):
    """Export JSON file to Excel format"""
    
    # Auto-generate output filename if not provided
    if output_file is None:
        input_path = Path(input_file)
        output_file = input_path.with_suffix('.xlsx')
    
    print(f"📂 Loading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle both single object and array
    records = data if isinstance(data, list) else [data]
    print(f"📊 Found {len(records)} records")
    
    # Remove large fields that don't work well in Excel
    exclude_fields = ['embedding', 'embeddings', 'vector']
    clean_records = []
    for record in records:
        clean = {k: v for k, v in record.items() 
                 if k.lower() not in exclude_fields}
        clean_records.append(clean)
    
    # Convert to DataFrame
    df = pd.DataFrame(clean_records)
    print(f"📋 Created DataFrame: {len(df)} rows × {len(df.columns)} columns")
    
    # Show first few columns
    if len(df.columns) > 0:
        preview_cols = ', '.join(df.columns[:8])
        if len(df.columns) > 8:
            preview_cols += '...'
        print(f"   Columns: {preview_cols}")
    
    # Export to Excel
    print(f"💾 Exporting to {output_file}...")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_excel(output_file, index=False, engine='openpyxl')
    
    file_size = output_path.stat().st_size / (1024 * 1024)  # MB
    print(f"✅ Done! Exported to: {output_file}")
    print(f"   File size: {file_size:.2f} MB")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python export_to_excel.py <input.json> [output.xlsx]")
        print("\nExamples:")
        print("  python export_to_excel.py data.json")
        print("  python export_to_excel.py data.json output.xlsx")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    export_json_to_excel(input_file, output_file)
