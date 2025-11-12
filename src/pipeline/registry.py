"""Pipeline step registry with all available steps."""
from typing import Callable, Dict, Any
from pathlib import Path
import json


class StepRegistry:
    """Registry of all available pipeline steps."""
    
    def __init__(self):
        self._steps: Dict[str, Callable] = {}
        self._services = {}  # Cache for lazy-loaded services
        self._register_default_steps()
    
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
        csv_service = self._get_service('csv')
        input_file = kwargs['input_file']
        output_file = kwargs['output_file']
        
        result = csv_service.normalize_file(
            input_file=input_file,
            output_file=output_file
        )
        
        return {
            'status': 'success',
            'output_file': output_file,
            'rows_processed': result.get('rows', 0)
        }
    
    def _crawl_projects(self, **kwargs) -> Dict[str, Any]:
        """Crawl project pages."""
        crawler_service = self._get_service('crawler')
        base_url = kwargs['base_url']
        project_url = kwargs.get('project_url')
        
        result = crawler_service.crawl_projects(
            base_url=base_url,
            project_url=project_url
        )
        
        return {
            'status': 'success',
            'projects_crawled': result.get('count', 0),
            'output_dir': result.get('output_dir')
        }
    
    def _crawl_professionals(self, **kwargs) -> Dict[str, Any]:
        """Crawl professional/member pages."""
        crawler_service = self._get_service('crawler')
        base_url = kwargs['base_url']
        directory_url = kwargs['directory_url']
        max_members = kwargs.get('max_members', 100)
        
        result = crawler_service.crawl_professionals(
            base_url=base_url,
            directory_url=directory_url,
            max_members=max_members
        )
        
        return {
            'status': 'success',
            'professionals_crawled': result.get('count', 0),
            'output_dir': result.get('output_dir')
        }
    
    def _parse_html(self, **kwargs) -> Dict[str, Any]:
        """Parse HTML files to JSON."""
        parser_service = self._get_service('parser')
        batch_mode = kwargs.get('batch_mode', True)
        
        if batch_mode:
            input_dir = kwargs['input_dir']
            output_dir = kwargs['output_dir']
            save_json = kwargs.get('save_json', True)
            
            result = parser_service.parse_html_batch(
                input_dir=input_dir,
                output_dir=output_dir,
                save_json=save_json
            )
        else:
            input_file = kwargs['input_file']
            output_file = kwargs.get('output_file')
            
            result = parser_service.parse_html_file(
                input_file=input_file,
                output_file=output_file
            )
        
        return {
            'status': 'success',
            'files_processed': result.get('count', 0),
            'output_dir': result.get('output_dir')
        }
    
    def _transform_data(self, **kwargs) -> Dict[str, Any]:
        """Transform and merge data."""
        transform_service = self._get_service('transform')
        input_file = kwargs['input_file']
        output_file = kwargs['output_file']
        transformations = kwargs.get('transformations', [])
        
        result = transform_service.transform(
            input_file=input_file,
            output_file=output_file,
            transformations=transformations
        )
        
        return {
            'status': 'success',
            'output_file': output_file,
            'records_processed': result.get('count', 0)
        }
    
    def _generate_embeddings(self, **kwargs) -> Dict[str, Any]:
        """Generate vector embeddings for text data."""
        embedding_service = self._get_service('embedding')
        input_file = kwargs['input_file']
        text_column = kwargs['text_column']
        model = kwargs.get('model', 'sbert')
        output_file = kwargs['output_file']
        
        result = embedding_service.generate_embeddings(
            input_file=input_file,
            text_column=text_column,
            model=model,
            output_file=output_file
        )
        
        return {
            'status': 'success',
            'output_file': output_file,
            'embeddings_generated': result.get('count', 0)
        }
    
    def _load_opensearch(self, **kwargs) -> Dict[str, Any]:
        """Load data to OpenSearch."""
        opensearch_service = self._get_service('opensearch')
        input_file = kwargs['input_file']
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 9200)
        index_name = kwargs['index_name']
        auth = kwargs.get('auth')
        
        result = opensearch_service.load_data(
            input_file=input_file,
            host=host,
            port=port,
            index_name=index_name,
            auth=auth
        )
        
        return {
            'status': 'success',
            'records_loaded': result.get('count', 0),
            'index_name': index_name
        }
    
    def _load_neo4j(self, **kwargs) -> Dict[str, Any]:
        """Load data to Neo4j."""
        neo4j_service = self._get_service('neo4j')
        input_file = kwargs['input_file']
        uri = kwargs.get('uri', 'bolt://localhost:7687')
        auth = kwargs.get('auth')
        
        result = neo4j_service.load_data(
            input_file=input_file,
            uri=uri,
            auth=auth
        )
        
        return {
            'status': 'success',
            'nodes_created': result.get('nodes', 0),
            'relationships_created': result.get('relationships', 0)
        }


_registry = StepRegistry()

def get_registry() -> StepRegistry:
    """Get the global step registry."""
    return _registry