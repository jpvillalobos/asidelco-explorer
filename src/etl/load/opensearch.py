"""
OpenSearch bulk loading primitives.
"""
from typing import Any, Dict, Iterable, List, Optional
import logging

from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import OpenSearchException

logger = logging.getLogger(__name__)


class OpenSearchLoader:
    """Small import-safe wrapper around opensearch-py."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_ssl: bool = False,
        verify_certs: bool = False,
        timeout: int = 60,
    ):
        self.host = host
        self.port = port

        config = {
            "hosts": [{"host": host, "port": port}],
            "use_ssl": use_ssl,
            "verify_certs": verify_certs,
            "ssl_show_warn": False,
            "timeout": timeout,
            "max_retries": 3,
            "retry_on_timeout": True,
        }

        if username and password:
            config["http_auth"] = (username, password)

        self.client = OpenSearch(**config)
        info = self.client.info()
        version = info.get("version", {}).get("number", "unknown")
        logger.info("Connected to OpenSearch %s at %s:%s", version, host, port)

    def create_index(
        self,
        index_name: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Create an index if it does not exist.

        When mappings/settings are omitted, OpenSearch index templates can apply
        automatically. That is the preferred path for ASIDELCO.
        """
        try:
            if self.client.indices.exists(index=index_name):
                logger.info("Index '%s' already exists", index_name)
                return True

            body: Dict[str, Any] = {}
            if settings:
                body["settings"] = settings
            if mappings:
                body["mappings"] = mappings

            self.client.indices.create(index=index_name, body=body)
            logger.info("Created index: %s", index_name)
            return True
        except OpenSearchException as exc:
            logger.error("Error creating index '%s': %s", index_name, exc)
            return False

    def bulk_index(
        self,
        index_name: str,
        documents: Iterable[Dict[str, Any]],
        id_field: Optional[str] = None,
        chunk_size: int = 500,
        request_timeout: int = 120,
    ) -> Dict[str, Any]:
        """Bulk index an iterable of documents."""
        indexed = 0
        failed = 0
        errors: List[Any] = []

        def actions():
            for doc in documents:
                action = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_source": doc,
                }
                if id_field and doc.get(id_field) not in (None, ""):
                    action["_id"] = doc[id_field]
                yield action

        for ok, item in helpers.streaming_bulk(
            self.client,
            actions(),
            chunk_size=chunk_size,
            request_timeout=request_timeout,
            raise_on_error=False,
            raise_on_exception=False,
        ):
            if ok:
                indexed += 1
                continue

            failed += 1
            if len(errors) < 10:
                errors.append(item)

        result = {
            "success": failed == 0,
            "indexed": indexed,
            "failed": failed,
            "errors": errors,
        }

        logger.info(
            "Indexed %s documents to '%s' (%s failed)",
            indexed,
            index_name,
            failed,
        )
        return result

    def delete_index(self, index_name: str) -> bool:
        """Delete an index if it exists."""
        try:
            if not self.client.indices.exists(index=index_name):
                logger.warning("Index '%s' does not exist", index_name)
                return False

            self.client.indices.delete(index=index_name)
            logger.info("Deleted index: %s", index_name)
            return True
        except OpenSearchException as exc:
            logger.error("Error deleting index '%s': %s", index_name, exc)
            return False

    def search(
        self,
        index_name: str,
        query: Dict[str, Any],
        size: int = 10,
    ) -> Dict[str, Any]:
        """Search documents in an index."""
        try:
            return self.client.search(index=index_name, body={"query": query, "size": size})
        except OpenSearchException as exc:
            logger.error("Error searching index '%s': %s", index_name, exc)
            return {"hits": {"total": {"value": 0}, "hits": []}}

    def close(self):
        """Close the OpenSearch connection."""
        self.client.close()


def load_to_opensearch(
    host: str = "localhost",
    port: int = 9200,
    username: Optional[str] = None,
    password: Optional[str] = None,
    use_ssl: bool = False,
    verify_certs: bool = False,
) -> OpenSearchLoader:
    """Factory kept for the service registry and CLI wrappers."""
    return OpenSearchLoader(
        host=host,
        port=port,
        username=username,
        password=password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
    )
