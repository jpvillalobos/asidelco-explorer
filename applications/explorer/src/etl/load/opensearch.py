"""
OpenSearch Loader
"""
from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import OpenSearchException
from typing import Dict, List, Any, Optional
from pathlib import Path
import json
import logging

# Setup logging properly
logger = logging.getLogger(__name__)

# Only create log file if logs directory exists, otherwise use console only
log_dir = Path(__file__).parent.parent.parent.parent / "logs"
if log_dir.exists():
    log_path = log_dir / "opensearch_upload.log"
    # Configure file handler
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
else:
    # Just log to console if logs directory doesn't exist
    logger.warning(f"Logs directory not found: {log_dir}. Logging to console only.")

logger.setLevel(logging.INFO)


class OpenSearchLoader:
    """Load data into OpenSearch"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_ssl: bool = False,
        verify_certs: bool = False
    ):
        """
        Initialize OpenSearch connection
        
        Args:
            host: OpenSearch host
            port: OpenSearch port
            username: Optional username for auth
            password: Optional password for auth
            use_ssl: Whether to use SSL
            verify_certs: Whether to verify SSL certificates
        """
        self.host = host
        self.port = port
        
        # Build connection config
        config = {
            'hosts': [{'host': host, 'port': port}],
            'use_ssl': use_ssl,
            'verify_certs': verify_certs,
            'ssl_show_warn': False
        }
        
        # Add authentication if provided
        if username and password:
            config['http_auth'] = (username, password)
        
        try:
            self.client = OpenSearch(**config)
            # Test connection
            info = self.client.info()
            logger.info(f"Connected to OpenSearch: {info.get('version', {}).get('number', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to connect to OpenSearch: {e}")
            self.client = None
    
    def create_index(
        self,
        index_name: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create an index with optional mappings and settings
        
        Args:
            index_name: Name of the index to create
            mappings: Index mappings
            settings: Index settings
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            logger.error("OpenSearch client not initialized")
            return False
        
        try:
            if self.client.indices.exists(index=index_name):
                logger.info(f"Index '{index_name}' already exists")
                return True
            
            body = {}
            if settings:
                body['settings'] = settings
            if mappings:
                body['mappings'] = mappings
            
            self.client.indices.create(index=index_name, body=body)
            logger.info(f"Created index: {index_name}")
            return True
            
        except OpenSearchException as e:
            logger.error(f"Error creating index '{index_name}': {e}")
            return False
    
    def bulk_index(
        self,
        index_name: str,
        documents: List[Dict[str, Any]],
        id_field: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Bulk index documents
        
        Args:
            index_name: Index name
            documents: List of documents to index
            id_field: Optional field name to use as document ID
            
        Returns:
            Dictionary with indexing results
        """
        if not self.client:
            logger.error("OpenSearch client not initialized")
            return {"success": False, "error": "Client not initialized"}
        
        if not documents:
            logger.warning("No documents to index")
            return {"success": True, "indexed": 0}
        
        try:
            # Prepare bulk actions
            actions = []
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                
                # Add document ID if specified
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                
                actions.append(action)
            
            # Execute bulk indexing
            success, failed = helpers.bulk(
                self.client,
                actions,
                raise_on_error=False,
                raise_on_exception=False
            )
            
            result = {
                "success": True,
                "indexed": success,
                "failed": len(failed) if failed else 0,
                "total": len(documents)
            }
            
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")
                result["errors"] = failed[:10]  # Log first 10 errors
            
            logger.info(f"Indexed {success}/{len(documents)} documents to '{index_name}'")
            return result
            
        except Exception as e:
            logger.error(f"Error during bulk indexing: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_index(self, index_name: str) -> bool:
        """Delete an index"""
        if not self.client:
            logger.error("OpenSearch client not initialized")
            return False
        
        try:
            if self.client.indices.exists(index=index_name):
                self.client.indices.delete(index=index_name)
                logger.info(f"Deleted index: {index_name}")
                return True
            else:
                logger.warning(f"Index '{index_name}' does not exist")
                return False
                
        except OpenSearchException as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            return False
    
    def search(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 10
    ) -> Dict[str, Any]:
        """
        Search documents in an index
        
        Args:
            index_name: Index to search
            query: OpenSearch query DSL
            size: Number of results to return
            
        Returns:
            Search results
        """
        if not self.client:
            logger.error("OpenSearch client not initialized")
            return {"hits": {"total": {"value": 0}, "hits": []}}
        
        try:
            response = self.client.search(
                index=index_name,
                body={"query": query, "size": size}
            )
            return response
            
        except OpenSearchException as e:
            logger.error(f"Error searching index '{index_name}': {e}")
            return {"hits": {"total": {"value": 0}, "hits": []}}
    
    def close(self):
        """Close the OpenSearch connection"""
        if self.client:
            self.client.close()
            logger.info("Closed OpenSearch connection")

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
