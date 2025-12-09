"""
Neo4j Service with Progress Reporting
"""
from typing import List, Dict, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)


class Neo4jService:
    """Service for Neo4j operations with progress reporting"""
    
    def __init__(self, uri: str, username: str, password: str):
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = None
        
        # Initialize Neo4j driver only when needed
        try:
            from neo4j import GraphDatabase
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
        except ImportError:
            logger.warning("Neo4j driver not available. Install with: pip install neo4j")
        except Exception as e:
            logger.error(f"Could not connect to Neo4j: {e}")
    
    def load_data(
        self,
        data: Union[List[Dict[str, Any]], Any],
        context: Optional[object] = None,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Load data into Neo4j with progress reporting
        
        Args:
            data: List of data dictionaries or pandas DataFrame
            context: Optional context object for reporting progress
            batch_size: Number of records per batch
            
        Returns:
            Summary of the load operation
        """
        if not self.driver:
            raise RuntimeError("Neo4j driver not available")
        
        # Convert data to list if needed
        if hasattr(data, 'to_dict'):
            # Handle pandas DataFrame
            data_list = data.to_dict('records')
        elif isinstance(data, list):
            data_list = data
        else:
            raise ValueError("Data must be a list of dictionaries or pandas DataFrame")
        
        total_records = len(data_list)
        loaded = 0
        errors = []
        
        if context:
            context.report_progress(0, total_records, "Starting Neo4j load")
        
        # Process in batches
        for i in range(0, total_records, batch_size):
            batch = data_list[i:i + batch_size]
            
            try:
                with self.driver.session() as session:
                    # Your Neo4j load logic here
                    # For now, just simulate loading
                    for record in batch:
                        # session.write_transaction(self._create_node, record)
                        pass
                
                loaded += len(batch)
                
                if context:
                    context.report_progress(
                        loaded,
                        total_records,
                        f"Loaded {loaded}/{total_records} records",
                        {
                            "loaded": loaded,
                            "errors": len(errors),
                            "current_batch": i // batch_size + 1
                        }
                    )
                
            except Exception as e:
                logger.error(f"Error loading batch {i // batch_size}: {e}")
                errors.append({"batch": i // batch_size, "error": str(e)})
        
        summary = {
            "total_loaded": loaded,
            "total_errors": len(errors),
            "success_rate": (loaded / total_records) * 100 if total_records > 0 else 0
        }
        
        if context:
            context.report_progress(
                total_records,
                total_records,
                f"Load complete: {loaded}/{total_records} records",
                summary
            )
        
        logger.info(f"Neo4j load completed: {loaded} records loaded")
        return summary
    
    def close(self):
        """Close the driver connection"""
        if self.driver:
            self.driver.close()