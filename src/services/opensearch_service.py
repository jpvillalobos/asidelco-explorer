"""
OpenSearch Service with Progress Reporting
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class OpenSearchService:
    """Service for OpenSearch operations with progress reporting"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        """Initialize OpenSearch service"""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.loader = None
    
    def _get_loader(self):
        """Lazy load the OpenSearch loader"""
        if self.loader is None:
            try:
                from etl.load.opensearch import load_to_opensearch
                self.loader = load_to_opensearch(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password
                )
            except ImportError as e:
                logger.error(f"Could not import OpenSearchLoader: {e}")
                raise RuntimeError(f"OpenSearchLoader not available: {e}")
        return self.loader
    
    def bulk_index(
        self,
        data: List[Dict[str, Any]],
        index_name: str,
        context: Optional[object] = None,
        batch_size: int = 100,
        id_field: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Bulk index data into OpenSearch with progress reporting
        
        Args:
            data: List of documents to index
            index_name: Name of the index
            context: Pipeline context for progress reporting
            batch_size: Number of documents per batch
            id_field: Field to use as document ID
            
        Returns:
            Dictionary with indexing results
        """
        total_records = len(data)
        indexed = 0
        errors = []
        
        if context:
            context.report_progress(0, total_records, "Starting OpenSearch indexing")
        
        try:
            loader = self._get_loader()
            
            # Ensure index exists
            if context:
                context.report_progress(0, total_records, f"Creating index '{index_name}'")
            
            loader.create_index(index_name)
            
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = data[i:i + batch_size]
                
                try:
                    result = loader.bulk_index(
                        index_name=index_name,
                        documents=batch,
                        id_field=id_field
                    )
                    
                    if result.get("success"):
                        indexed += result.get("indexed", 0)
                    else:
                        errors.append(result.get("error", "Unknown error"))
                    
                    if context:
                        context.report_progress(
                            min(indexed, total_records),
                            total_records,
                            f"Indexed {indexed}/{total_records} documents",
                            {
                                "indexed": indexed,
                                "errors": len(errors),
                                "current_batch": i // batch_size + 1
                            }
                        )
                    
                except Exception as e:
                    logger.error(f"Error indexing batch {i // batch_size}: {e}")
                    errors.append(str(e))
            
            summary = {
                "total_indexed": indexed,
                "total_errors": len(errors),
                "success_rate": (indexed / total_records) * 100 if total_records > 0 else 0,
                "index_name": index_name
            }
            
            if context:
                context.report_progress(
                    total_records,
                    total_records,
                    f"Indexing complete: {indexed}/{total_records} documents",
                    summary
                )
            
            logger.info(f"OpenSearch indexing completed: {indexed} documents indexed")
            return summary
            
        except Exception as e:
            logger.error(f"OpenSearch indexing failed: {e}")
            raise
    
    def search(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 10,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Search documents in OpenSearch
        
        Args:
            index_name: Index to search
            query: Search query
            size: Number of results
            context: Pipeline context
            
        Returns:
            Search results
        """
        try:
            loader = self._get_loader()
            
            if context:
                context.report_progress(0, 100, f"Searching index '{index_name}'")
            
            results = loader.search(index_name, query, size)
            
            if context:
                hits = results.get("hits", {}).get("total", {}).get("value", 0)
                context.report_progress(
                    100,
                    100,
                    f"Search complete: {hits} results found"
                )
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    def delete_index(self, index_name: str) -> bool:
        """Delete an index"""
        try:
            loader = self._get_loader()
            return loader.delete_index(index_name)
        except Exception as e:
            logger.error(f"Failed to delete index: {e}")
            return False