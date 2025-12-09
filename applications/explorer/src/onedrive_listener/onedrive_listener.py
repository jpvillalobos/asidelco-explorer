import os
import sys
import time
import logging
from io import BytesIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple
import uuid

import requests
import pandas as pd
from msal import ConfidentialClientApplication

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.blob import BlobBlock
from azure.core.exceptions import ResourceExistsError, ServiceResponseError

LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("./logs/onedrive_listener.log")
    ],
)
logger = logging.getLogger("onedrive_listener")
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.DEBUG)

# Graph / OneDrive
GRAPH = "https://graph.microsoft.com/v1.0"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
DRIVE_ID = os.getenv("DRIVE_ID")
FOLDER_ID = os.getenv("ONEDRIVE_FOLDER_ID")

# Blob
BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "ingestion")
CSV_PREFIX = os.getenv("CSV_PREFIX", "csv")

# Blob upload tuning
BLOB_UPLOAD_TIMEOUT = int(os.getenv("BLOB_UPLOAD_TIMEOUT", "600"))  # 10min default
MAX_UPLOAD_RETRIES = int(os.getenv("MAX_UPLOAD_RETRIES", "3"))
BLOB_CHUNK_SIZE = int(os.getenv("BLOB_CHUNK_SIZE", str(256 * 1024)))  # 256KB default

# ETL API
ETL_API_BASE = os.getenv("ETL_API_BASE", "http://localhost:8000")
NORMALIZE_REMOVE_DUPES = os.getenv("NORMALIZE_REMOVE_DUPES", "true").lower() == "true"
NORMALIZE_MISSING_STRATEGY = os.getenv("NORMALIZE_MISSING_STRATEGY", "drop")
ID_FROM_COLUMN = os.getenv("ID_FROM_COLUMN", "proyecto")
ID_COLUMN_NAME = os.getenv("ID_COLUMN_NAME", "id")
ID_SEPARATOR = os.getenv("ID_SEPARATOR", "-")
ID_START = int(os.getenv("ID_START", "1"))

# Polling
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
STARTUP_TIME = datetime.now(timezone.utc)

# Skip baseline sync flag
SKIP_BASELINE_SYNC = os.getenv("SKIP_BASELINE_SYNC", "true").lower() == "true"

# Blob client with increased timeout (use default transport with connection config)
blob_svc: Optional[BlobServiceClient] = None
if BLOB_CONN_STR:
    blob_svc = BlobServiceClient.from_connection_string(
        BLOB_CONN_STR,
        connection_timeout=60,
        read_timeout=BLOB_UPLOAD_TIMEOUT,
        max_single_put_size=4 * 1024 * 1024,  # 4MB max single upload
        max_block_size=4 * 1024 * 1024,        # 4MB chunks for block upload
    )
else:
    logger.warning("AZURE_STORAGE_CONNECTION_STRING not set; Blob upload will fail.")

def get_token() -> str:
    app = ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Token error: {result}")
    return result["access_token"]

def get_folder_delta(token: str, folder_id: str, delta_link: Optional[str]) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if delta_link:
        url = delta_link
    else:
        url = f"{GRAPH}/drives/{DRIVE_ID}/items/{folder_id}/delta?$select=id,name,file,deleted,createdDateTime,lastModifiedDateTime,size"
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def download_excel_to_memory(token: str, item_id: str) -> BytesIO:
    url = f"{GRAPH}/drives/{DRIVE_ID}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {token}"}
    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        bio = BytesIO()
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                bio.write(chunk)
        bio.seek(0)
        return bio

def excel_bytes_to_csv_bytes(xlsx_bytes: BytesIO) -> bytes:
    df = pd.read_excel(xlsx_bytes)
    return df.to_csv(index=False).encode("utf-8")

def upload_csv_to_blob(blob_path: str, data: bytes) -> str:
    """Upload CSV to blob using staged blocks with small chunks to avoid socket timeouts."""
    if not blob_svc:
        raise RuntimeError("BlobServiceClient not configured")

    logger.info(f"[UPLOAD START] Blob: {blob_path}, Size: {len(data)} bytes ({len(data)/(1024*1024):.2f}MB)")
    container = blob_svc.get_container_client(BLOB_CONTAINER)
    try:
        logger.debug(f"[UPLOAD] Checking container: {BLOB_CONTAINER}")
        container.create_container()
        logger.info(f"[UPLOAD] Container created: {BLOB_CONTAINER}")
    except ResourceExistsError:
        logger.debug(f"[UPLOAD] Container exists: {BLOB_CONTAINER}")
    except Exception as e:
        logger.debug(f"[UPLOAD] Container check (non-fatal): {e}")

    blob = container.get_blob_client(blob_path)

    # Always use chunked upload with small chunks
    chunk_size = max(64 * 1024, BLOB_CHUNK_SIZE)  # safety floor of 64KB
    total = len(data)

    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
        try:
            logger.info(f"[UPLOAD] Starting chunked upload with {chunk_size//1024}KB chunks (attempt {attempt}/{MAX_UPLOAD_RETRIES})")
            block_list = []
            offset = 0
            block_idx = 0
            start = time.time()

            while offset < total:
                end = min(offset + chunk_size, total)
                chunk = data[offset:end]
                block_id = str(uuid.uuid4())

                t0 = time.time()
                blob.stage_block(block_id=block_id, data=chunk, length=len(chunk))
                dt = time.time() - t0
                kbps = (len(chunk) / 1024) / dt if dt > 0 else 0.0
                logger.info(f"[UPLOAD] Staged block {block_idx+1}: {len(chunk)//1024}KB in {dt:.2f}s ({kbps:.0f} KB/s)")

                block_list.append(BlobBlock(block_id=block_id))
                offset = end
                block_idx += 1

            logger.info(f"[UPLOAD] Committing {len(block_list)} blocks...")
            blob.commit_block_list(block_list)
            total_dt = time.time() - start
            avg_kbps = (total / 1024) / total_dt if total_dt > 0 else 0.0
            logger.info(f"[UPLOAD] ✓ Completed in {total_dt:.2f}s (avg {avg_kbps:.0f} KB/s)")
            return f"{BLOB_CONTAINER}/{blob_path}"

        except (ServiceResponseError, TimeoutError, OSError) as e:
            elapsed = time.time() - start if 'start' in locals() else 0
            logger.error(f"[UPLOAD FAILED] After {elapsed:.2f}s: {e}")
            if attempt < MAX_UPLOAD_RETRIES:
                backoff = 2 ** attempt
                logger.warning(f"[UPLOAD] Retrying in {backoff}s... (attempt {attempt}/{MAX_UPLOAD_RETRIES})")
                time.sleep(backoff)
            else:
                logger.error(f"[UPLOAD] ❌ All {MAX_UPLOAD_RETRIES} attempts exhausted")
                raise

def parse_conn_string(conn: str) -> Tuple[str, str]:
    """Extract account name and key from connection string"""
    parts = dict(p.split("=", 1) for p in conn.split(";") if "=" in p)
    return parts.get("AccountName"), parts.get("AccountKey")

def make_blob_sas_url(container: str, blob_path: str, minutes: int = 60) -> str:
    account_name, account_key = parse_conn_string(BLOB_CONN_STR or "")
    if not account_name or not account_key:
        raise RuntimeError("Cannot parse storage account credentials from connection string")
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_path,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=minutes),
    )
    endpoint = f"https://{account_name}.blob.core.windows.net"
    return f"{endpoint}/{container}/{blob_path}?{sas}"

def call_etl_normalize_from_url(sas_url: str, out_filename: str):
    """POST to ETL API /api/csv/normalize-from-url"""
    url = f"{ETL_API_BASE}/api/csv/normalize-from-url"
    payload = {
        "url": sas_url,
        "output_filename": out_filename,
        "missing_value_strategy": NORMALIZE_MISSING_STRATEGY,
        "remove_duplicates": NORMALIZE_REMOVE_DUPES,
        "id_from_column": ID_FROM_COLUMN,
        "id_column_name": ID_COLUMN_NAME,
        "id_separator": ID_SEPARATOR,
        "id_start": ID_START,
    }
    r = requests.post(url, json=payload, timeout=600)
    r.raise_for_status()
    return r.json()

def should_process_item(item: dict) -> Tuple[bool, str]:
    """Check if item should be processed (delta API already filters by change tracking)"""
    if item.get("deleted"):
        return False, "Deleted"
    if not item.get("file"):
        return False, "Not a file"
    name = item.get("name", "")
    if not name.lower().endswith(".xlsx"):
        return False, "Not an Excel file"
    
    # No timestamp check - delta API already returns only changed items since last sync
    return True, "OK"

def process_item(token: str, item: dict):
    name = item.get("name", "file.xlsx")
    stem = Path(name).stem
    logger.info(f"Processing new file: {name}")

    # 1) Download Excel
    xlsx = download_excel_to_memory(token, item["id"])
    logger.info(f"Downloaded {name} ({xlsx.getbuffer().nbytes} bytes)")

    # 2) Convert to CSV
    csv_bytes = excel_bytes_to_csv_bytes(xlsx)
    logger.info(f"Converted to CSV ({len(csv_bytes)} bytes)")

    # 3) Upload CSV to Blob
    blob_path = f"{CSV_PREFIX}/{stem}.csv"
    full_blob = upload_csv_to_blob(blob_path, csv_bytes)
    logger.info(f"Uploaded CSV to blob: {full_blob}")

    # 4) Generate SAS URL
    sas_url = make_blob_sas_url(BLOB_CONTAINER, blob_path)
    logger.info("Generated SAS URL")

    # 5) Call ETL API to normalize from URL
    out_filename = f"output/{stem}_normalized.csv"
    try:
        result = call_etl_normalize_from_url(sas_url, out_filename)
        logger.info(f"ETL normalize result: {result}")
    except Exception as e:
        logger.error(f"ETL API call failed for {name}: {e}. CSV uploaded but not normalized.")
        # Don't re-raise; allow delta to progress

def test_blob_connection():
    """Test blob storage connectivity and credentials"""
    logger.info("=" * 60)
    logger.info("Testing Azure Blob Storage connection...")
    logger.info("=" * 60)
    
    if not blob_svc:
        logger.error("❌ BlobServiceClient not initialized - check AZURE_STORAGE_CONNECTION_STRING")
        return False
    
    try:
        # Test 1: List containers (remove max_results to avoid kwargs issue)
        logger.info("Test 1: Listing containers...")
        containers = list(blob_svc.list_containers())[:5]  # Take first 5 after listing
        logger.info(f"✓ Found {len(containers)} container(s)")
        for c in containers:
            logger.info(f"  - {c.name}")
        
        # Test 2: Get/create target container
        logger.info(f"\nTest 2: Checking container '{BLOB_CONTAINER}'...")
        container = blob_svc.get_container_client(BLOB_CONTAINER)
        try:
            props = container.get_container_properties()
            logger.info(f"✓ Container exists: {props.name}")
        except ResourceExistsError:
            logger.info(f"✓ Container exists (from exception)")
        except Exception as e:
            if "ContainerNotFound" in str(e):
                logger.info(f"⚠ Container '{BLOB_CONTAINER}' does not exist, creating...")
                container.create_container()
                logger.info(f"✓ Container created: {BLOB_CONTAINER}")
            else:
                raise
        
        # Test 3: Upload a small test blob
        logger.info(f"\nTest 3: Uploading test blob...")
        test_blob_name = f"{CSV_PREFIX}/connection-test-{int(time.time())}.txt"
        test_data = f"Connection test at {datetime.now(timezone.utc).isoformat()}".encode("utf-8")
        
        blob_client = container.get_blob_client(test_blob_name)
        blob_client.upload_blob(test_data, overwrite=True, timeout=30)
        logger.info(f"✓ Test blob uploaded: {test_blob_name}")
        
        # Test 4: Read back the blob
        logger.info(f"\nTest 4: Reading test blob...")
        download = blob_client.download_blob(timeout=30)
        content = download.readall()
        logger.info(f"✓ Test blob downloaded: {len(content)} bytes")
        logger.info(f"  Content: {content.decode('utf-8')}")
        
        # Test 5: Generate SAS URL
        logger.info(f"\nTest 5: Generating SAS URL...")
        sas_url = make_blob_sas_url(BLOB_CONTAINER, test_blob_name, minutes=5)
        logger.info(f"✓ SAS URL generated (valid for 5min)")
        logger.info(f"  URL: {sas_url[:80]}...")
        
        # Test 6: Access via SAS URL
        logger.info(f"\nTest 6: Testing SAS URL access...")
        r = requests.get(sas_url, timeout=10)
        r.raise_for_status()
        logger.info(f"✓ SAS URL accessible: {len(r.content)} bytes")
        
        # Clean up test blob
        logger.info(f"\nCleaning up test blob...")
        blob_client.delete_blob()
        logger.info(f"✓ Test blob deleted")
        
        logger.info("=" * 60)
        logger.info("✅ All blob storage tests passed!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ Blob storage test failed: {e}")
        logger.exception("Full traceback:")
        logger.error("=" * 60)
        return False

def main():
    logger.info("Starting OneDrive listener")
    logger.info(f"Drive: {DRIVE_ID} Folder: {FOLDER_ID}")
    logger.info(f"Polling every {POLL_INTERVAL}s")

    Path("./logs").mkdir(parents=True, exist_ok=True)

    # Run blob connection test before starting the main loop
    if not test_blob_connection():
        logger.error("Blob storage connection test failed. Exiting.")
        sys.exit(1)
    
    logger.info("\nStarting main listener loop...\n")
    
    token = get_token()
    delta_link: Optional[str] = None
    
    # If SKIP_BASELINE_SYNC=true, do one empty baseline sync to initialize delta_link
    if SKIP_BASELINE_SYNC:
        logger.info("Skipping baseline sync - establishing delta link without processing existing files")
        changes = get_folder_delta(token, FOLDER_ID, None)
        delta_link = changes.get("@odata.deltaLink")
        logger.info("Delta link established; now monitoring for new changes only")

    while True:
        try:
            changes = get_folder_delta(token, FOLDER_ID, delta_link)
            items = changes.get("value", [])
            logger.info(f"Delta returned {len(items)} items")

            for item in items:
                ok, reason = should_process_item(item)
                if not ok:
                    logger.debug(f"Skip {item.get('name')}: {reason}")
                    continue
                try:
                    process_item(token, item)
                except Exception as e:
                    logger.exception(f"Failed processing {item.get('name')}: {e}")
                    # Continue to next item; don't block delta progression

            # Update delta link immediately after processing this page
            new_delta = changes.get("@odata.deltaLink")
            if new_delta:
                delta_link = new_delta
                logger.debug(f"Updated delta link")

            # Pagination
            next_link = changes.get("@odata.nextLink")
            while next_link:
                r = requests.get(next_link, headers={"Authorization": f"Bearer {token}"}, timeout=60)
                r.raise_for_status()
                page = r.json()
                for item in page.get("value", []):
                    ok, reason = should_process_item(item)
                    if not ok:
                        logger.debug(f"Skip {item.get('name')}: {reason}")
                        continue
                    try:
                        process_item(token, item)
                    except Exception as e:
                        logger.exception(f"Failed processing {item.get('name')}: {e}")
                        # Continue; don't block
                
                # Update delta after each paginated page
                page_delta = page.get("@odata.deltaLink")
                if page_delta:
                    delta_link = page_delta
                    logger.debug("Updated delta link from page")
                
                next_link = page.get("@odata.nextLink")

            time.sleep(POLL_INTERVAL)
        
        except Exception as e:
            logger.exception("Loop error; refreshing token and retrying after delay")
            time.sleep(30)
            try:
                token = get_token()
            except Exception:
                logger.error("Token refresh failed; backing off")
                time.sleep(60)

if __name__ == "__main__":
    main()