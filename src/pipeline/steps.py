"""
Pipeline Step Definitions
"""
from enum import Enum
from typing import Any, Dict, Callable, List
from dataclasses import dataclass, field


class StepType(str, Enum):
    """Available pipeline steps"""
    # Extract steps
    EXTRACT_HTML = "extract_html"
    EXTRACT_CSV = "extract_csv"
    CRAWL_PROFESSIONALS = "crawl_professionals"
    CRAWL_PROJECTS = "crawl_projects"
    
    # Transform steps
    PARSE_HTML = "parse_html"
    PARSE_MEMBERS = "parse_members"
    ENRICH_DATA = "enrich_data"
    MERGE_EXCEL = "merge_excel"
    NORMALIZE_CSV = "normalize_csv"
    
    # Embedding steps
    GENERATE_EMBEDDINGS = "generate_embeddings"
    
    # Load steps
    LOAD_NEO4J = "load_neo4j"
    LOAD_OPENSEARCH = "load_opensearch"
    EXPORT_EXCEL = "export_excel"


@dataclass
class StepConfig:
    """Configuration for a pipeline step"""
    name: str
    step_type: StepType
    handler: Callable
    required_args: List[str] = field(default_factory=list)
    optional_args: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[StepType] = field(default_factory=list)
    description: str = ""