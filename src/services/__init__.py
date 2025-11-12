"""
Services module with graceful error handling
"""
import logging

logger = logging.getLogger(__name__)

# Import services with error handling
__all__ = []

try:
    from .csv_service import CSVService
    __all__.append('CSVService')
except Exception as e:
    logger.warning(f"Could not import CSVService: {e}")

try:
    from .crawler_service import CrawlerService
    __all__.append('CrawlerService')
except Exception as e:
    logger.warning(f"Could not import CrawlerService: {e}")

try:
    from .embedding_service import EmbeddingService
    __all__.append('EmbeddingService')
except Exception as e:
    logger.warning(f"Could not import EmbeddingService: {e}")

try:
    from .neo4j_service import Neo4jService
    __all__.append('Neo4jService')
except Exception as e:
    logger.warning(f"Could not import Neo4jService: {e}")

try:
    from .opensearch_service import OpenSearchService
    __all__.append('OpenSearchService')
except Exception as e:
    logger.warning(f"Could not import OpenSearchService: {e}")

try:
    from .parser_service import ParserService
    __all__.append('ParserService')
except Exception as e:
    logger.warning(f"Could not import ParserService: {e}")

try:
    from .storage_service import StorageService
    __all__.append('StorageService')
except Exception as e:
    logger.warning(f"Could not import StorageService: {e}")

try:
    from .transform_service import TransformService
    __all__.append('TransformService')
except Exception as e:
    logger.warning(f"Could not import TransformService: {e}")

logger.info(f"Successfully imported {len(__all__)} services: {', '.join(__all__)}")