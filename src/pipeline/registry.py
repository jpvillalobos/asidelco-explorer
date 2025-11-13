"""Pipeline step registry with all available steps."""
from typing import Callable, Dict, Any
from pathlib import Path
import json
import logging
from datetime import datetime


class StepRegistry:
    """Registry of all available pipeline steps."""

    def __init__(self):
        self._steps: Dict[str, Callable] = {}
        self._services = {}  # Cache for lazy-loaded services
        self._register_default_steps()
        self._workspace_dir = None  # Will be set when needed

    def set_workspace_dir(self, workspace_dir: str):
        """Set workspace directory for logging."""
        self._workspace_dir = Path(workspace_dir)

    def _setup_step_logging(self, service_name: str, step_name: str) -> logging.FileHandler:
        """Setup logging for a specific service/step combination."""
        if not self._workspace_dir:
            return None

        logs_dir = self._workspace_dir / 'logs'
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create log file for this service-step combination
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = logs_dir / f"{service_name}-{step_name}_{timestamp}.log"

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)

        return file_handler

    def _get_service(self, service_name: str):
        """Lazy load services only when needed."""
        if service_name not in self._services:
            if service_name == 'csv':
                from services.csv_service import CSVService
                self._services['csv'] = CSVService()
            elif service_name == 'crawler':
                from services.crawler_service import CrawlerService
                self._services['crawler'] = CrawlerService()
            elif service_name == 'parser':
                from services.parser_service import ParserService
                self._services['parser'] = ParserService()
            elif service_name == 'transform':
                from services.transform_service import TransformService
                self._services['transform'] = TransformService()
            elif service_name == 'embedding':
                from services.embedding_service import EmbeddingService
                self._services['embedding'] = EmbeddingService()
            elif service_name == 'opensearch':
                from services.opensearch_service import OpenSearchService
                self._services['opensearch'] = OpenSearchService()
            elif service_name == 'neo4j':
                from services.neo4j_service import Neo4jService
                self._services['neo4j'] = Neo4jService()
        return self._services[service_name]
    
    def _register_default_steps(self):
        """Register all default pipeline steps."""
        # Ingest Stage
        self.register('normalize_csv', self._normalize_csv)
        
        # Crawl Stage
        self.register('crawl_projects', self._crawl_projects)
        self.register('crawl_professionals', self._crawl_professionals)
        
        # Transform Stage
        self.register('parse_html', self._parse_html)
        self.register('transform_data', self._transform_data)
        
        # Enhance Stage
        self.register('generate_embeddings', self._generate_embeddings)
        
        # Load Stage
        self.register('load_opensearch', self._load_opensearch)
        self.register('load_neo4j', self._load_neo4j)
    
    def register(self, name: str, func: Callable):
        """Register a new step function."""
        self._steps[name] = func
    
    def get(self, name: str) -> Callable:
        """Get a registered step function."""
        if name not in self._steps:
            raise ValueError(f"Step '{name}' not found in registry")
        return self._steps[name]
    
    def list_steps(self) -> list:
        """List all registered step names."""
        return list(self._steps.keys())
    
    # Step implementations - services loaded on demand
    
    def _normalize_csv(self, **kwargs) -> Dict[str, Any]:
        """Normalize input CSV/Excel file."""
        # Setup step-specific logging
        file_handler = self._setup_step_logging('csv', 'normalize_csv')
        logger = logging.getLogger('services.csv_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            csv_service = self._get_service('csv')
            input_file = kwargs['input_file']
            output_file = kwargs['output_file']

            logger.info(f"Starting CSV normalization: {input_file} -> {output_file}")

            # Call normalize_csv method on CSVService
            df = csv_service.normalize_csv(
                input_file=input_file,
                output_file=output_file,
                context=None
            )

            result = {
                'status': 'success',
                'output_file': output_file,
                'rows_processed': len(df) if df is not None else 0
            }

            logger.info(f"CSV normalization completed: {result['rows_processed']} rows processed")
            return result

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()
    
    def _crawl_projects(self, **kwargs) -> Dict[str, Any]:
        """Crawl project pages."""
        file_handler = self._setup_step_logging('crawler', 'crawl_projects')
        logger = logging.getLogger('services.crawler_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            crawler_service = self._get_service('crawler')
            input_file = kwargs.get('input_file')
            base_url = kwargs['base_url']
            project_url = kwargs.get('project_url')
            output_dir = kwargs.get('output_dir')
            rate_limit = kwargs.get('rate_limit', 0.5)
            max_retries = kwargs.get('max_retries', 3)
            timeout = kwargs.get('timeout', 30)

            logger.info(f"Starting project crawl: {base_url}")
            logger.info(f"Input file: {input_file}")
            logger.info(f"Output directory: {output_dir}")

            result = crawler_service.crawl_projects(
                input_file=input_file,
                base_url=base_url,
                project_url=project_url,
                output_dir=output_dir,
                rate_limit=rate_limit,
                max_retries=max_retries,
                timeout=timeout
            )

            logger.info(f"Crawl completed: {result.get('count', 0)} projects")

            return {
                'status': 'success',
                'projects_crawled': result.get('count', 0),
                'output_dir': result.get('output_dir')
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()
    
    def _crawl_professionals(self, **kwargs) -> Dict[str, Any]:
        """Crawl professional/member pages."""
        file_handler = self._setup_step_logging('crawler', 'crawl_professionals')
        logger = logging.getLogger('services.crawler_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            crawler_service = self._get_service('crawler')
            input_dir = kwargs.get('input_dir')
            base_url = kwargs['base_url']
            directory_url = kwargs['directory_url']
            max_members = kwargs.get('max_members', 100)
            output_dir = kwargs.get('output_dir')
            rate_limit = kwargs.get('rate_limit', 0.5)
            max_retries = kwargs.get('max_retries', 3)
            timeout = kwargs.get('timeout', 30)

            logger.info(f"Starting professionals crawl: {directory_url}, max={max_members}")
            logger.info(f"Input directory: {input_dir}")
            logger.info(f"Output directory: {output_dir}")

            result = crawler_service.crawl_professionals(
                input_dir=input_dir,
                base_url=base_url,
                directory_url=directory_url,
                max_members=max_members,
                output_dir=output_dir,
                rate_limit=rate_limit,
                max_retries=max_retries,
                timeout=timeout
            )

            logger.info(f"Crawl completed: {result.get('count', 0)} professionals")

            return {
                'status': 'success',
                'professionals_crawled': result.get('count', 0),
                'output_dir': result.get('output_dir')
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()
    
    def _parse_html(self, **kwargs) -> Dict[str, Any]:
        """Parse HTML files to JSON."""
        file_handler = self._setup_step_logging('parser', 'parse_html')
        logger = logging.getLogger('services.parser_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            parser_service = self._get_service('parser')
            batch_mode = kwargs.get('batch_mode', True)

            if batch_mode:
                input_dir = kwargs['input_dir']
                output_dir = kwargs['output_dir']
                save_json = kwargs.get('save_json', True)

                logger.info(f"Starting batch HTML parsing: {input_dir} -> {output_dir}")

                result = parser_service.parse_html_batch(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    save_json=save_json
                )
            else:
                input_file = kwargs['input_file']
                output_file = kwargs.get('output_file')

                logger.info(f"Starting HTML parsing: {input_file}")

                result = parser_service.parse_html_file(
                    input_file=input_file,
                    output_file=output_file
                )

            logger.info(f"HTML parsing completed: {result.get('count', 0)} files processed")

            return {
                'status': 'success',
                'files_processed': result.get('count', 0),
                'output_dir': result.get('output_dir')
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()
    
    def _transform_data(self, **kwargs) -> Dict[str, Any]:
        """Transform and merge data."""
        file_handler = self._setup_step_logging('transform', 'transform_data')
        logger = logging.getLogger('services.transform_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            transform_service = self._get_service('transform')
            input_file = kwargs['input_file']
            output_file = kwargs['output_file']
            transformations = kwargs.get('transformations', [])

            logger.info(f"Starting data transformation: {input_file} -> {output_file}")

            result = transform_service.transform(
                input_file=input_file,
                output_file=output_file,
                transformations=transformations
            )

            logger.info(f"Transformation completed: {result.get('count', 0)} records processed")

            return {
                'status': 'success',
                'output_file': output_file,
                'records_processed': result.get('count', 0)
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()

    def _generate_embeddings(self, **kwargs) -> Dict[str, Any]:
        """Generate vector embeddings for text data."""
        file_handler = self._setup_step_logging('embedding', 'generate_embeddings')
        logger = logging.getLogger('services.embedding_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            embedding_service = self._get_service('embedding')
            input_file = kwargs['input_file']
            text_column = kwargs['text_column']
            model = kwargs.get('model', 'sbert')
            output_file = kwargs['output_file']

            logger.info(f"Starting embedding generation: model={model}, column={text_column}")

            result = embedding_service.generate_embeddings(
                input_file=input_file,
                text_column=text_column,
                model=model,
                output_file=output_file
            )

            logger.info(f"Embedding generation completed: {result.get('count', 0)} embeddings")

            return {
                'status': 'success',
                'output_file': output_file,
                'embeddings_generated': result.get('count', 0)
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()

    def _load_opensearch(self, **kwargs) -> Dict[str, Any]:
        """Load data to OpenSearch."""
        file_handler = self._setup_step_logging('opensearch', 'load_opensearch')
        logger = logging.getLogger('services.opensearch_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            opensearch_service = self._get_service('opensearch')
            input_file = kwargs['input_file']
            host = kwargs.get('host', 'localhost')
            port = kwargs.get('port', 9200)
            index_name = kwargs['index_name']
            auth = kwargs.get('auth')

            logger.info(f"Starting OpenSearch load: {host}:{port}/{index_name}")

            result = opensearch_service.load_data(
                input_file=input_file,
                host=host,
                port=port,
                index_name=index_name,
                auth=auth
            )

            logger.info(f"OpenSearch load completed: {result.get('count', 0)} records loaded")

            return {
                'status': 'success',
                'records_loaded': result.get('count', 0),
                'index_name': index_name
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()

    def _load_neo4j(self, **kwargs) -> Dict[str, Any]:
        """Load data to Neo4j."""
        file_handler = self._setup_step_logging('neo4j', 'load_neo4j')
        logger = logging.getLogger('services.neo4j_service')

        if file_handler:
            logger.addHandler(file_handler)

        try:
            neo4j_service = self._get_service('neo4j')
            input_file = kwargs['input_file']
            uri = kwargs.get('uri', 'bolt://localhost:7687')
            auth = kwargs.get('auth')

            logger.info(f"Starting Neo4j load: {uri}")

            result = neo4j_service.load_data(
                input_file=input_file,
                uri=uri,
                auth=auth
            )

            logger.info(f"Neo4j load completed: {result.get('nodes', 0)} nodes, {result.get('relationships', 0)} relationships")

            return {
                'status': 'success',
                'nodes_created': result.get('nodes', 0),
                'relationships_created': result.get('relationships', 0)
            }

        finally:
            if file_handler:
                logger.removeHandler(file_handler)
                file_handler.close()


_registry = StepRegistry()

def get_registry() -> StepRegistry:
    """Get the global step registry."""
    return _registry