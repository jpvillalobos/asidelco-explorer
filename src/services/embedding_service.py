"""
Embedding Service
"""
from typing import List, Dict, Any, Optional, Union
import logging
import json
from pathlib import Path
import os
from openai import OpenAI

# Set up module-level logger
logger = logging.getLogger(__name__)

class EmbeddingService:
    """Service for generating text embeddings (OpenAI only)"""

    def __init__(self, provider: str = "openai", model: Optional[str] = None, api_key: Optional[str] = None, **kwargs):
        provider = (provider or "openai").lower()
        if provider != "openai":
            raise RuntimeError("Only OpenAI embeddings are supported currently. Use provider='openai'.")
        
        # Initialize OpenAI client
        self.model = model or "text-embedding-3-small"
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY not set. Pass api_key or export OPENAI_API_KEY.")
        
        self.client = OpenAI(api_key=self.api_key, **kwargs)
        self.dimension = 1536  # for text-embedding-3-small
        
        logger.info(f"EmbeddingService initialized with provider='{provider}', model='{self.model}'")
        logger.debug(f"Using embedding dimension: {self.dimension}")

    def generate_embedding(self, text: str, **kwargs) -> List[float]:
        response = self.client.embeddings.create(model=self.model, input=text, **kwargs)
        return response.data[0].embedding

    def generate_embeddings_batch(self, texts: List[str], batch_size: int = 32, show_progress: bool = False) -> List[List[float]]:
        embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            response = self.client.embeddings.create(model=self.model, input=batch)
            embeddings.extend([data.embedding for data in response.data])
        return embeddings

    def generate_documents_embeddings(self, documents: List[Dict], text_field: str = "text", embedding_field: str = "embedding") -> List[Dict]:
        texts = [doc.get(text_field, "") for doc in documents]
        embeddings = self.generate_embeddings_batch(texts)
        for doc, emb in zip(documents, embeddings):
            doc[embedding_field] = emb
        return documents

    def get_embedding_dimension(self) -> int:
        return self.dimension

    def generate_embeddings(self, input_file: str, text_field: str = None, text_column: str = None, 
                          model: str = None, output_file: str = None,
                          force_regenerate: bool = False, **kwargs) -> Dict[str, Any]:
        """
        Generate embeddings from a JSON file and save to output file.
        Supports both text_field and text_column parameter names.
        """
        
        # Handle both parameter names
        field_name = text_field or text_column
        if not field_name:
            raise ValueError("Either text_field or text_column must be provided")
        
        # Load input data
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        logger.info(f"Loading data from {input_file}")
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Ensure data is a list
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError(f"Input data must be a list or dict, got {type(data)}")
        
        total_records = len(data)
        logger.info(f"Loaded {total_records} records from {input_file}")
        
        # Set output path
        output_path = Path(output_file) if output_file else input_path.with_suffix('.embeddings.json')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Count existing embeddings
        existing_embeddings = sum(1 for doc in data if 'embedding' in doc and doc['embedding'])
        if existing_embeddings > 0:
            logger.info(f"Found {existing_embeddings} records with existing embeddings")
        
        # Generate embeddings for each document
        processed_count = 0
        skipped_count = 0
        new_embeddings_count = 0
        batch_save_interval = 100
        
        logger.info(f"Starting embedding generation for field '{field_name}'")
        logger.info(f"Force regenerate existing embeddings: {force_regenerate}")
        
        for idx, doc in enumerate(data):
            # Skip if embedding already exists
            if not force_regenerate and 'embedding' in doc and doc['embedding']:
                skipped_count += 1
                logger.debug(f"Skipping record {idx + 1}/{total_records} - embedding already exists")
                processed_count += 1
                continue
            
            # Check if text field exists and has content
            if field_name in doc and doc[field_name]:
                text = str(doc[field_name])
                logger.debug(f"Processing record {idx + 1}/{total_records} - text length: {len(text)}")
                
                try:
                    embedding = self.generate_embedding(text)
                    doc['embedding'] = embedding
                    doc['embedding_model'] = self.model
                    new_embeddings_count += 1
                    processed_count += 1
                    
                    if new_embeddings_count % 10 == 0:
                        logger.info(f"Generated {new_embeddings_count} new embeddings, {skipped_count} skipped, {processed_count}/{total_records} total processed")
                    
                    # Save incrementally every batch_save_interval new embeddings
                    if new_embeddings_count % batch_save_interval == 0:
                        logger.info(f"Saving intermediate results after {new_embeddings_count} new embeddings...")
                        with open(output_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        logger.debug(f"Intermediate save completed to {output_path}")
                        
                except Exception as e:
                    logger.error(f"Error generating embedding for record {idx + 1}: {str(e)}")
                    processed_count += 1
            else:
                logger.debug(f"Skipping record {idx + 1}/{total_records} - no text in field '{field_name}'")
                processed_count += 1
        
        # Final save
        logger.info(f"Saving final results to {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Embedding generation completed: {new_embeddings_count} new embeddings generated, {skipped_count} skipped, {processed_count} total processed")
        
        return {
            'status': 'success',
            'total_records': total_records,
            'new_embeddings': new_embeddings_count,
            'skipped': skipped_count,
            'processed': processed_count,
            'output_file': str(output_path)
        }

def load_data(input_file):
    """Load data from file."""
    with open(input_file, 'r') as f:
        return json.load(f)
