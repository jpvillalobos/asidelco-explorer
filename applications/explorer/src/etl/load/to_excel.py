import os
import json
import time
import pandas as pd
from datetime import datetime

# Configuration (adjust as needed)
ENHANCED_JSON_DIR = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/enhanced"
ERROR_LOG_PATH = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/excel_export_errors.log"
OUTPUT_EXCEL_PATH = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/all_projects.xlsx"

def log_error(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_PATH, "a", encoding="utf-8") as log:
        log.write(f"[{timestamp}] {message}\n")

def normalize_keys(d):
    return {
        k.strip().lower().replace(" ", "_"): v
        for k, v in d.items()
        if k.lower() != "embedding"
    }

def load_all_projects_to_excel(directory, output_path):
    if not os.path.exists(directory):
        print(f"Directory not found: {directory}")
        return

    files = sorted(f for f in os.listdir(directory) if f.endswith(".json"))
    total = len(files)
    count = 0
    start_time = time.time()
    records = []

    print(f"Starting Excel export of {total} project files...")

    for filename in files:
        count += 1
        filepath = os.path.join(directory, filename)

        try:
            with open(filepath, "r", encoding="utf-8") as file:
                data = json.load(file)
                normalized = normalize_keys(data)
                records.append(normalized)
                print(f"[{count}/{total}] Processed: {filename}")
        except Exception as e:
            log_error(f"Failed to process {filename}: {e}")
            print(f"[{count}/{total}] Error processing {filename} (logged)")

    df = pd.DataFrame(records)
    df.to_excel(output_path, index=False)
    elapsed = time.time() - start_time
    print(f"Excel export completed in {elapsed:.2f} seconds. Output saved to: {output_path}")

if __name__ == "__main__":
    load_all_projects_to_excel(ENHANCED_JSON_DIR, OUTPUT_EXCEL_PATH)
