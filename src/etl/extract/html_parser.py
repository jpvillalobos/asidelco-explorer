# html_to_json_parser.py
import os
import json
from bs4 import BeautifulSoup

def extract_tables_from_response(raw_html):
    try:
        html_start = raw_html.index('<h1>')
    except ValueError:
        return []

    html_content = raw_html[html_start:]
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table", id="datos")
    extracted_data = []

    for table in tables:
        table_data = {}
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) == 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                table_data[key] = value
        extracted_data.append(table_data)

    return extracted_data

def parse_project_html_file(file_path):
    """Parse HTML file into a JSON dictionary (including project_id)."""
    with open(file_path, "r", encoding="utf-8") as f:
        raw_html = f.read()

    parsed_tables = extract_tables_from_response(raw_html)
    project_id = os.path.basename(file_path).replace(".html", "")

    if parsed_tables:
        doc = {"project_id": project_id}
        for table in parsed_tables:
            doc.update(table)
        return doc
    else:
        return None
