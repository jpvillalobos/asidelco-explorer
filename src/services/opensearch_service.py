"""
OpenSearch Service with Progress Reporting
"""
from typing import List, Dict, Any, Optional, Iterable
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)


class OpenSearchService:
    """Service for OpenSearch operations with progress reporting"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_ssl: bool = False,
        verify_certs: bool = False,
    ):
        """Initialize OpenSearch service"""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.verify_certs = verify_certs
        self.loader = None

    def _get_loader(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify_certs: Optional[bool] = None,
    ):
        """Lazy load the OpenSearch loader"""
        requested = {
            "host": host or self.host,
            "port": port or self.port,
            "username": username if username is not None else self.username,
            "password": password if password is not None else self.password,
            "use_ssl": self.use_ssl if use_ssl is None else use_ssl,
            "verify_certs": self.verify_certs if verify_certs is None else verify_certs,
        }

        if self.loader is None or getattr(self, "_loader_config", None) != requested:
            try:
                from etl.load.opensearch import load_to_opensearch
                self.loader = load_to_opensearch(
                    **requested
                )
                self._loader_config = requested
            except ImportError as e:
                logger.error(f"Could not import OpenSearchLoader: {e}")
                raise RuntimeError(f"OpenSearchLoader not available: {e}")
        return self.loader

    def load_data(
        self,
        input_file: str,
        index_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        auth: Optional[Dict[str, str]] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_ssl: bool = False,
        verify_certs: bool = False,
        batch_size: int = 500,
        id_field: str = "record_id",
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
        skip_not_ready: bool = True,
        progress_interval: int = 5000,
        chunk_size: int = 1024 * 1024,
        context: Optional[object] = None,
    ) -> Dict[str, Any]:
        """Stream a JSON array into OpenSearch without loading it all in memory."""
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        if auth:
            username = auth.get("username") or auth.get("user") or username
            password_env = auth.get("password_env") or auth.get("passwordEnv")
            password = auth.get("password") or (os.getenv(password_env) if password_env else None) or password

        loader = self._get_loader(
            host=host,
            port=port,
            username=username,
            password=password,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
        )

        if not loader.create_index(index_name, mappings=mappings, settings=settings):
            raise RuntimeError(f"Could not create or access OpenSearch index: {index_name}")

        stats = {
            "count": 0,
            "indexed": 0,
            "failed": 0,
            "skipped_not_ready": 0,
            "index_name": index_name,
        }
        errors: List[Any] = []

        if context:
            context.report_progress(0, 0, f"Starting OpenSearch load into '{index_name}'")

        for batch in self._iter_indexable_batches(
            input_path=input_path,
            batch_size=batch_size,
            chunk_size=chunk_size,
            skip_not_ready=skip_not_ready,
            stats=stats,
        ):
            result = loader.bulk_index(
                index_name=index_name,
                documents=batch,
                id_field=id_field,
                chunk_size=batch_size,
            )

            stats["indexed"] += result.get("indexed", 0)
            stats["failed"] += result.get("failed", 0)
            if result.get("errors"):
                errors.extend(result["errors"][: max(0, 10 - len(errors))])

            if progress_interval and stats["count"] % progress_interval < batch_size:
                message = (
                    f"Indexed {stats['indexed']} records "
                    f"({stats['failed']} failed, {stats['skipped_not_ready']} skipped)"
                )
                logger.info(message)
                if context:
                    context.report_progress(stats["count"], 0, message, stats.copy())

        summary = {
            **stats,
            "status": "success" if stats["failed"] == 0 else "partial_success",
            "success_rate": (
                stats["indexed"] / (stats["indexed"] + stats["failed"]) * 100
                if (stats["indexed"] + stats["failed"]) else 0
            ),
        }
        if errors:
            summary["errors"] = errors

        logger.info(
            "OpenSearch load completed: %s indexed, %s failed, %s skipped",
            stats["indexed"],
            stats["failed"],
            stats["skipped_not_ready"],
        )

        if context:
            context.report_progress(stats["count"], stats["count"], "OpenSearch load completed", summary)

        return summary

    def _iter_indexable_batches(
        self,
        input_path: Path,
        batch_size: int,
        chunk_size: int,
        skip_not_ready: bool,
        stats: Dict[str, int],
    ) -> Iterable[List[Dict[str, Any]]]:
        from services.validation_enrichment_service import ValidationEnrichmentService

        batch: List[Dict[str, Any]] = []
        iterator = ValidationEnrichmentService()._iter_json_array(input_path, chunk_size=chunk_size)

        for record in iterator:
            stats["count"] += 1
            if skip_not_ready and not record.get("index_ready", {}).get("is_ready", True):
                stats["skipped_not_ready"] += 1
                continue

            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
    
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
                "indexed": indexed,
                "total_errors": len(errors),
                "failed": len(errors),
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
