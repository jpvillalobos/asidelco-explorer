"""
OpenSearch Service - Load JSON data into OpenSearch
"""
from typing import Dict, Any, List, Optional, Tuple
import logging
import json
from pathlib import Path
from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import RequestError, ConnectionError as OSConnectionError
import time

logger = logging.getLogger(__name__)


class OpenSearchService:
    """Service for loading data into OpenSearch"""
    
    def __init__(self):
        self.client = None
    
    def _create_client(
        self,
        host: str = 'localhost',
        port: int = 9200,
        auth: Optional[Tuple[str, str]] = None,
        use_ssl: bool = False,
        verify_certs: bool = False,
        ssl_show_warn: bool = False
    ) -> OpenSearch:
        """
        Create OpenSearch client.
        
        Args:
            host: OpenSearch host
            port: OpenSearch port
            auth: Optional tuple of (username, password)
            use_ssl: Use SSL/TLS
            verify_certs: Verify SSL certificates
            ssl_show_warn: Show SSL warnings
            
        Returns:
            OpenSearch client instance
        """
        logger.info(f"Creating OpenSearch client: {host}:{port}")
        
        client_config = {
            'hosts': [{'host': host, 'port': port}],
            'use_ssl': use_ssl,
            'verify_certs': verify_certs,
            'ssl_show_warn': ssl_show_warn,
            'timeout': 30,
            'max_retries': 3,
            'retry_on_timeout': True
        }
        
        if auth:
            client_config['http_auth'] = auth
            logger.info(f"Using authentication: {auth[0]}:***")
        
        try:
            client = OpenSearch(**client_config)
            
            # Test connection
            info = client.info()
            logger.info(f"Connected to OpenSearch cluster: {info.get('cluster_name', 'unknown')}")
            logger.info(f"OpenSearch version: {info.get('version', {}).get('number', 'unknown')}")
            
            return client
            
        except Exception as e:
            logger.error(f"Failed to create OpenSearch client: {e}", exc_info=True)
            raise ConnectionError(f"Cannot connect to OpenSearch at {host}:{port}: {e}")
    
    def _check_cluster_health(self, client: OpenSearch) -> Dict[str, Any]:
        """
        Check cluster health and disk space.
        
        Args:
            client: OpenSearch client
            
        Returns:
            Dict with health status
        """
        try:
            health = client.cluster.health()
            logger.info(f"Cluster status: {health.get('status')}")
            logger.info(f"Active shards: {health.get('active_shards')}")
            logger.info(f"Number of nodes: {health.get('number_of_nodes')}")
            
            # Check for blocks
            settings = client.cluster.get_settings()
            transient_blocks = settings.get('transient', {}).get('cluster', {}).get('blocks', {})
            persistent_blocks = settings.get('persistent', {}).get('cluster', {}).get('blocks', {})
            
            if transient_blocks or persistent_blocks:
                logger.warning(f"Cluster has blocks: transient={transient_blocks}, persistent={persistent_blocks}")
            
            return {
                'status': health.get('status'),
                'healthy': health.get('status') in ['green', 'yellow'],
                'blocks': transient_blocks or persistent_blocks
            }
            
        except Exception as e:
            logger.error(f"Failed to check cluster health: {e}", exc_info=True)
            return {'status': 'unknown', 'healthy': False, 'error': str(e)}
    
    def _create_index_with_mapping(
        self,
        client: OpenSearch,
        index_name: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create index with mappings if it doesn't exist.
        
        Args:
            client: OpenSearch client
            index_name: Name of the index
            mappings: Optional index mappings
            settings: Optional index settings
            
        Returns:
            True if index was created or already exists
        """
        try:
            # Check if index exists
            if client.indices.exists(index=index_name):
                logger.info(f"Index '{index_name}' already exists")
                return True
            
            # Default settings optimized for text search and vector similarity
            default_settings = {
                'number_of_shards': 1,
                'number_of_replicas': 0,
                'refresh_interval': '1s',
                'analysis': {
                    'analyzer': {
                        'spanish_analyzer': {
                            'type': 'standard',
                            'stopwords': '_spanish_'
                        }
                    }
                }
            }
            
            # Default mappings based on sample JSON structure
            default_mappings = {
                'properties': {
                    'record_id': {'type': 'keyword'},
                    'csv_id': {'type': 'keyword'},
                    'csv_proyecto': {'type': 'long'},
                    'csv_exonerado': {'type': 'keyword'},
                    'csv_area': {'type': 'float'},
                    'csv_obra': {'type': 'keyword'},
                    'csv_subobra': {'type': 'keyword'},
                    'csv_fechaproyecto': {'type': 'date', 'format': 'strict_date_optional_time||yyyy-MM-dd'},
                    'csv_provincia': {'type': 'keyword'},
                    'csv_canton': {'type': 'keyword'},
                    'csv_distrito': {'type': 'keyword'},
                    'csv_unidad': {'type': 'keyword'},
                    'csv_clasificacion': {'type': 'keyword'},
                    
                    # Project fields
                    'project_project_id': {'type': 'keyword'},
                    'project_num_proyecto': {'type': 'keyword'},
                    'project_fecha_proyecto': {'type': 'date', 'format': 'strict_date_optional_time||yyyy-MM-dd'},
                    'project_estado': {'type': 'keyword'},
                    'project_detalle_de_estado': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'project_cedula_propietario': {'type': 'keyword'},
                    'project_nombre_propietario': {'type': 'text', 'analyzer': 'spanish_analyzer', 'fields': {'keyword': {'type': 'keyword'}}},
                    'project_num_apc': {'type': 'keyword'},
                    'project_num_boleta': {'type': 'keyword'},
                    'project_estado_de_la_boleta': {'type': 'keyword'},
                    'project_catastro': {'type': 'keyword'},
                    'project_descripcion_del_proyecto': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'project_clasificacion': {'type': 'keyword'},
                    'project_direccion_exacta': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'project_provincia': {'type': 'keyword'},
                    'project_canton': {'type': 'keyword'},
                    'project_distrito': {'type': 'keyword'},
                    'project_carnet_profesional': {'type': 'keyword'},
                    'project_carnet_empresa': {'type': 'keyword'},
                    'project_responsable': {'type': 'text', 'analyzer': 'spanish_analyzer', 'fields': {'keyword': {'type': 'keyword'}}},
                    'project_descripcion': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'project_tasado': {'type': 'long'},
                    'project_constancia_de_recibido': {'type': 'keyword'},
                    'project_monto_pendiente_de_pago': {'type': 'long'},
                    'project_pagar_proyecto': {'type': 'keyword'},
                    'project_detalle': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    
                    # Professional fields
                    'professional_cedula': {'type': 'keyword'},
                    'professional_carne': {'type': 'keyword'},
                    'professional_nombrecompleto': {'type': 'text', 'analyzer': 'spanish_analyzer', 'fields': {'keyword': {'type': 'keyword'}}},
                    'professional_colegio': {'type': 'keyword'},
                    'professional_correopermanente': {'type': 'keyword'},
                    'professional_correolaboral': {'type': 'keyword'},
                    'professional_condicion': {'type': 'keyword'},
                    'professional_telcelular': {'type': 'keyword'},
                    'professional_teloficina': {'type': 'keyword'},
                    'professional_fax': {'type': 'keyword'},
                    'professional_lugar': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'professional_direccion': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'professional_carnet': {'type': 'keyword'},
                    
                    # Metadata fields
                    'metadata_merged_at': {'type': 'date'},
                    'metadata_row_index': {'type': 'long'},
                    'metadata_warnings': {'type': 'text'},
                    
                    # Geocoding fields
                    'latitude': {'type': 'float'},
                    'longitude': {'type': 'float'},
                    'location': {'type': 'geo_point'},  # For geo queries
                    'geocoded_address': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'geocoding_level': {'type': 'integer'},
                    'geocoding_description': {'type': 'keyword'},
                    'geocoding_status': {'type': 'keyword'},
                    
                    # AI Summary fields
                    'resumen': {'type': 'text', 'analyzer': 'spanish_analyzer'},
                    'summary_model': {'type': 'keyword'},
                    'summary_tokens': {'type': 'integer'},
                    
                    # Vector embeddings (if present)
                    'embedding': {
                        'type': 'knn_vector',
                        'dimension': 384,  # Default for sentence-transformers
                        'method': {
                            'name': 'hnsw',
                            'space_type': 'cosinesimil',
                            'engine': 'lucene'
                        }
                    }
                }
            }
            
            # Merge custom settings/mappings if provided
            final_settings = {**default_settings, **(settings or {})}
            final_mappings = {**default_mappings, **(mappings or {})}
            
            # Create index
            body = {
                'settings': final_settings,
                'mappings': final_mappings
            }
            
            logger.info(f"Creating index '{index_name}' with mappings")
            logger.debug(f"Index body: {json.dumps(body, indent=2)}")
            
            response = client.indices.create(index=index_name, body=body)
            
            logger.info(f"Index '{index_name}' created successfully")
            return True
            
        except RequestError as e:
            if 'resource_already_exists_exception' in str(e):
                logger.info(f"Index '{index_name}' already exists")
                return True
            else:
                logger.error(f"Failed to create index: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"Failed to create index: {e}", exc_info=True)
            raise
    
    def load_data(
        self,
        input_file: str,
        host: str = 'localhost',
        port: int = 9200,
        index_name: str = 'asidelco-explorer',
        auth: Optional[Tuple[str, str]] = None,
        use_ssl: bool = False,
        verify_certs: bool = False,
        batch_size: int = 500,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Load JSON data into OpenSearch.
        
        Args:
            input_file: Path to input JSON file (array of records)
            host: OpenSearch host
            port: OpenSearch port
            index_name: Target index name
            auth: Optional tuple of (username, password)
            use_ssl: Use SSL/TLS
            verify_certs: Verify SSL certificates
            batch_size: Bulk index batch size
            mappings: Optional custom index mappings
            settings: Optional custom index settings
            context: Optional context for progress reporting
            
        Returns:
            Dict with status, count, and stats
        """
        logger.info("Starting OpenSearch data loading")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Host: {host}:{port}")
        logger.info(f"  Index: {index_name}")
        logger.info(f"  Batch size: {batch_size}")
        
        # Validate input file
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input JSON file not found: {input_file}")
        
        # Load JSON data
        logger.info(f"Loading JSON data from {input_file}")
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load JSON file: {e}", exc_info=True)
            raise ValueError(f"Invalid JSON file: {e}")
        
        # Validate JSON is a list
        if not isinstance(json_data, list):
            raise ValueError(f"JSON data must be an array of objects, got {type(json_data).__name__}")
        
        total_records = len(json_data)
        logger.info(f"Loaded {total_records} records from JSON")
        
        if total_records == 0:
            logger.warning("No records found in JSON file")
            return {
                'status': 'warning',
                'message': 'No records to process',
                'count': 0,
                'stats': {}
            }
        
        # Create OpenSearch client
        client = self._create_client(
            host=host,
            port=port,
            auth=auth,
            use_ssl=use_ssl,
            verify_certs=verify_certs
        )
        
        # Check cluster health
        health = self._check_cluster_health(client)
        if not health.get('healthy'):
            logger.warning(f"Cluster is not healthy: {health.get('status')}")
            if health.get('blocks'):
                raise RuntimeError(
                    f"Cluster has blocks that prevent indexing: {health.get('blocks')}. "
                    "Check disk space and remove read-only blocks."
                )
        
        # Create index if it doesn't exist
        self._create_index_with_mapping(
            client=client,
            index_name=index_name,
            mappings=mappings,
            settings=settings
        )
        
        # Statistics
        stats = {
            'total_records': total_records,
            'indexed': 0,
            'failed': 0,
            'errors': []
        }
        
        # Prepare bulk actions
        logger.info(f"Preparing bulk index operations")
        
        def generate_actions():
            """Generator for bulk actions"""
            for i, record in enumerate(json_data):
                try:
                    if not isinstance(record, dict):
                        logger.warning(f"Record {i} is not a dict, skipping: {type(record).__name__}")
                        stats['failed'] += 1
                        continue
                    
                    # Prepare document
                    doc = record.copy()
                    
                    # Add geo_point field if lat/lon present
                    if 'latitude' in doc and 'longitude' in doc:
                        if doc['latitude'] and doc['longitude']:
                            doc['location'] = {
                                'lat': doc['latitude'],
                                'lon': doc['longitude']
                            }
                    
                    # Use record_id as document ID if available
                    doc_id = doc.get('record_id') or doc.get('csv_id') or f"doc_{i}"
                    
                    yield {
                        '_index': index_name,
                        '_id': doc_id,
                        '_source': doc
                    }
                    
                except Exception as e:
                    logger.error(f"Error preparing record {i}: {e}", exc_info=True)
                    stats['failed'] += 1
                    stats['errors'].append({'record': i, 'error': str(e)})
        
        # Bulk index with progress reporting
        logger.info(f"Starting bulk indexing in batches of {batch_size}")
        start_time = time.time()
        
        try:
            success_count = 0
            error_count = 0
            
            # Use bulk helper with progress tracking
            for ok, response in helpers.streaming_bulk(
                client,
                generate_actions(),
                chunk_size=batch_size,
                raise_on_error=False,
                raise_on_exception=False,
                max_retries=3,
                initial_backoff=2,
                max_backoff=600
            ):
                if ok:
                    success_count += 1
                    stats['indexed'] += 1
                else:
                    error_count += 1
                    stats['failed'] += 1
                    
                    # Log error details
                    action, response = list(response.items())[0]
                    error_msg = response.get('error', {})
                    logger.warning(f"Failed to index document: {error_msg}")
                    stats['errors'].append({
                        'action': action,
                        'error': error_msg
                    })
                
                # Report progress every 100 documents
                if context and (success_count + error_count) % 100 == 0:
                    context.report_progress(
                        success_count + error_count,
                        total_records,
                        f"Indexed {success_count}/{total_records} documents",
                        {'indexed': success_count, 'failed': error_count}
                    )
            
            elapsed_time = time.time() - start_time
            docs_per_sec = total_records / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(f"Bulk indexing completed")
            logger.info(f"  Indexed: {stats['indexed']}")
            logger.info(f"  Failed: {stats['failed']}")
            logger.info(f"  Time: {elapsed_time:.2f}s")
            logger.info(f"  Throughput: {docs_per_sec:.2f} docs/sec")
            
            # Refresh index to make documents searchable
            logger.info(f"Refreshing index '{index_name}'")
            client.indices.refresh(index=index_name)
            
            # Get final index stats
            index_stats = client.indices.stats(index=index_name)
            doc_count = index_stats['_all']['primaries']['docs']['count']
            logger.info(f"Index '{index_name}' now contains {doc_count} documents")
            
            stats['doc_count'] = doc_count
            stats['elapsed_time'] = elapsed_time
            stats['docs_per_sec'] = docs_per_sec
            
            if context:
                context.report_progress(
                    total_records,
                    total_records,
                    "OpenSearch loading complete",
                    stats
                )
            
            return {
                'status': 'success' if stats['failed'] == 0 else 'partial',
                'index_name': index_name,
                'count': stats['indexed'],
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}", exc_info=True)
            raise RuntimeError(f"Failed to load data to OpenSearch: {e}")
        
        finally:
            # Close client
            if client:
                client.close()
                logger.info("OpenSearch client closed")