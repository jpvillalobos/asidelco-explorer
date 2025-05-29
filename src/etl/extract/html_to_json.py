#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CFIA HTML Project Extractor – Adapted to New HTML Format

──────────────────────────────────────────────────────────────
• Extracts project data from a new HTML structure (from PanelResultado).
• Targets <section class="rubros"> → First table → <td>Label</td><td><p>Value</p></td>
• Handles nested <font> tags inside values.
• Logs errors and progress with elapsed time tracking.
"""

import os
import json
import traceback
from bs4 import BeautifulSoup
from datetime import datetime

# ────── Configuration ──────
INPUT_DIR = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/html"
OUTPUT_DIR = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/json"
ERROR_LOG = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/html-to-json-errors.log"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ────── Helpers ──────
def log_error(message):
    """Write error to log file with timestamp."""
    with open(ERROR_LOG, "a", encoding="utf-8") as log:
        log.write(f"[{datetime.now()}] {message}\n")

def normalize_text(text):
    """Clean and normalize whitespace."""
    return ' '.join(text.strip().split())

def extract_text(cell):
    """Extract text from <td>, including nested <p> or <font>."""
    if cell.find("p"):
        return normalize_text(cell.find("p").get_text())
    return normalize_text(cell.get_text())

# ────── Main Parser ──────
def parse_project_html_file(filepath):
    """
    Parse a CFIA HTML project file using updated structure.
    
    Returns:
        dict of extracted project fields
    Raises:
        ValueError if structure is unexpected
    """
    with open(filepath, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

    data = {}

    # Navigate to the project info table under <section class="rubros">
    rubros_section = soup.find("section", class_="rubros")
    if not rubros_section:
        raise ValueError("Missing <section class='rubros'>")

    project_table = rubros_section.find("table", class_="tableStyle2")
    if not project_table:
        raise ValueError("Missing <table class='tableStyle2'> inside rubros section")

    rows = project_table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 2:
            key = extract_text(cells[0])
            val = extract_text(cells[1])
            data[key] = val

    return data

# ────── Bulk Processor ──────
def convert_all_files():
    """Process all HTML files and write corresponding JSON files."""
    start_time = datetime.now()
    files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith(".html")])
    total_files = len(files)

    print(f"Processing {total_files} HTML files...\n")

    for idx, filename in enumerate(files, 1):
        try:
            input_path = os.path.join(INPUT_DIR, filename)
            data = parse_project_html_file(input_path)

            # Use project ID from content or fallback to filename
            project_id = data.get("Num. Proyecto", "").strip() or filename.replace(".html", "")
            output_path = os.path.join(OUTPUT_DIR, f"{project_id}.json")

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            elapsed = datetime.now() - start_time
            print(f"[{idx}/{total_files}] ✔ {filename} → {project_id}.json | Elapsed: {elapsed}")

        except Exception as e:
            error_msg = f"Error in file {filename}: {str(e)}\n{traceback.format_exc()}"
            log_error(error_msg)
            print(f"[{idx}/{total_files}] ✖ {filename} — error logged")

    print(f"\n✅ Finished in {datetime.now() - start_time}")

# ────── Entrypoint ──────
if __name__ == "__main__":
    convert_all_files()
