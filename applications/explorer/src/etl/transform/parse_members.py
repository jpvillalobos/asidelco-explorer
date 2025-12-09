import os
import json
from bs4 import BeautifulSoup

# Input directory with detail HTML files
input_dir = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/profesionals/html"
# Output directory to save JSON files
output_dir = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/profesionals/json"
os.makedirs(output_dir, exist_ok=True)

def extract_member_details(html):
    soup = BeautifulSoup(html, "html.parser")
    section = soup.select_one("section.container.documentsPage.seccionBuscador")

    if not section:
        return None

    data = {}

    # Extract input fields
    for input_tag in section.find_all("input", {"type": ["text"]}):
        key = input_tag.get("id") or input_tag.get("name")
        value = input_tag.get("value", "").strip()
        if key:
            data[key] = value

    # Extract textareas (like Lugar de Trabajo, Dirección Laboral)
    for textarea in section.find_all("textarea"):
        key = textarea.get("id") or textarea.get("name")
        value = textarea.text.strip()
        if key:
            data[key] = value

    return data

# Process each HTML file
for filename in os.listdir(input_dir):
    if not filename.endswith("-details.html"):
        continue

    path = os.path.join(input_dir, filename)
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()

    parsed_data = extract_member_details(html)

    if not parsed_data:
        print(f"Skipped {filename}: no valid section found")
        continue

    json_name = filename.replace(".html", ".json")
    output_path = os.path.join(output_dir, json_name)
    with open(output_path, "w", encoding="utf-8") as jf:
        json.dump(parsed_data, jf, ensure_ascii=False, indent=2)

    print(f"Parsed and saved: {json_name}")

print("All member detail files processed.")
