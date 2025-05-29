import os
import json
import time
import urllib3
from datetime import datetime
from opensearchpy import OpenSearch

# ─────────────────────────────────────────────
# Config paths
# ─────────────────────────────────────────────

enhanced_dir = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/enhanced"
log_path = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/logs/opensearch_upload.log"
index_name = "cfia-projects"
embedding_key = "text-embedding-3-small"
geo_pipeline = "cfia-add-geo"

# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────

log_file = open(log_path, "a", encoding="utf-8")

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg)
    log_file.write(full_msg + "\n")

# ─────────────────────────────────────────────
# Suppress InsecureRequestWarning for localhost
# ─────────────────────────────────────────────

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────
# OpenSearch client
# ─────────────────────────────────────────────

client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "OpenSearch123!"),  # Replace with your credentials
    use_ssl=True,
    verify_certs=False
)

# ─────────────────────────────────────────────
# Pre-check: Index existence
# ─────────────────────────────────────────────

if not client.indices.exists(index=index_name):
    log(f"ERROR: Index '{index_name}' does not exist. Create it manually before running this script.")
    log_file.close()
    exit(1)

log(f"Uploading documents to OpenSearch index '{index_name}'")

# ─────────────────────────────────────────────
# Sanitize field if it's empty, null, or invalid
# ─────────────────────────────────────────────

def clean_nullable_fields(doc, fields):
    for field in fields:
        if field in doc:
            val = doc[field].get("value", None)
            if val in ("", None) or isinstance(val, (list, dict)):
                del doc[field]

# ─────────────────────────────────────────────
# Process each file
# ─────────────────────────────────────────────

files = sorted([f for f in os.listdir(enhanced_dir) if f.endswith(".json")])
total_files = len(files)
processed = 0
start_time = time.time()

for file in files:
    processed += 1
    file_path = os.path.join(enhanced_dir, file)

    with open(file_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    project_id = doc.get("project_id", {}).get("value", f"unknown_{file}")

    # Skip if embedding is missing
    if "embeddings" not in doc or embedding_key not in doc["embeddings"]:
        log(f"[{processed}/{total_files}] Skipping {file}: missing '{embedding_key}'")
        continue

    # Skip if resumen is missing or empty
    resumen = doc.get("resumen", {}).get("value", "")
    if not resumen.strip():
        log(f"[{processed}/{total_files}] Skipping {file}: missing resumen.value")
        continue

    # ─────────────── Clean nullable fields ───────────────
    nullable_fields = [
        "fecha_proyecto",
        "fecha_ingreso_municipal"
        # Add more fields here if needed
    ]
    clean_nullable_fields(doc, nullable_fields)

    try:
        response = client.index(
            index=index_name,
            id=project_id,
            body=doc,
            pipeline=geo_pipeline
        )
        result = response.get("result", "unknown")
        elapsed = int(time.time() - start_time)
        hrs, rem = divmod(elapsed, 3600)
        mins, secs = divmod(rem, 60)
        elapsed_str = f"{hrs:02}:{mins:02}:{secs:02}"
        log(f"[{processed}/{total_files}] Indexed {file} (ID: {project_id}) - Result: {result} - Elapsed: {elapsed_str}")

    except Exception as e:
        log(f"[{processed}/{total_files}] Failed to index {file} (ID: {project_id}) - Error: {e}")

log("All documents processed.")
log_file.close()
