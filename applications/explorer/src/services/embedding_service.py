"""
Embedding Service - Generate text embeddings for documents
"""
from typing import List, Dict, Any, Optional
import logging
import json
from pathlib import Path
import time

from embeddings.sbert import generate_embeddings as create_sbert_embedder
from embeddings.openai import generate_openai_embeddings

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating text embeddings"""
    
    def __init__(self):
        self.embedder = None
        self.provider = None
        self.model = None
        self.dimension = None
    
    def _initialize_embedder(
        self,
        provider: str = "openai",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize embedding provider.
        
        Args:
            provider: Embedding provider ('openai' or 'sbert')
            model: Model name
            api_key: API key for OpenAI
            **kwargs: Additional provider-specific arguments
        """
        provider = provider.lower()
        logger.info(f"Initializing embedding provider: {provider}")
        
        if provider == "openai":
            model = model or "text-embedding-3-small"
            logger.info(f"Using OpenAI model: {model}")
            
            if not api_key:
                logger.warning("No API key provided for OpenAI, will use environment variable")
            
            try:
                self.embedder = generate_openai_embeddings(
                    api_key=api_key,
                    model=model,
                    **kwargs
                )
                self.provider = "openai"
                self.model = model
                
                # Get dimension from embedder
                self.dimension = getattr(self.embedder, 'dimension', None)
                
                # If not set, get from model name
                if not self.dimension:
                    if "text-embedding-3-small" in model:
                        self.dimension = 1536
                    elif "text-embedding-3-large" in model:
                        self.dimension = 3072
                    elif "text-embedding-ada-002" in model:
                        self.dimension = 1536
                    else:
                        self.dimension = 1536  # Default
                
                logger.info(f"OpenAI embedder initialized: dimension={self.dimension}")
                
            except Exception as e:
                logger.error(f"Failed to initialize OpenAI embedder: {e}", exc_info=True)
                raise RuntimeError(f"Cannot initialize OpenAI embedder: {e}")
        
        elif provider == "sbert":
            model = model or "all-MiniLM-L6-v2"
            logger.info(f"Using Sentence-BERT model: {model}")
            
            try:
                self.embedder = create_sbert_embedder(model=model, **kwargs)
                self.provider = "sbert"
                self.model = model
                
                # Get dimension from embedder
                self.dimension = getattr(self.embedder, 'dimension', 384)
                
                logger.info(f"SBERT embedder initialized: dimension={self.dimension}")
                
            except Exception as e:
                logger.error(f"Failed to initialize SBERT embedder: {e}", exc_info=True)
                raise RuntimeError(f"Cannot initialize SBERT embedder: {e}")
        
        else:
            raise ValueError(f"Unsupported embedding provider: {provider}. Use 'openai' or 'sbert'.")
    
    def generate_embeddings(
        self,
        input_file: str,
        output_file: str,
        text_field: str = "resumen",
        embedding_field: str = "embedding",
        provider: str = "openai",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        batch_size: int = 32,
        context: Optional[object] = None
    ) -> Dict[str, Any]:
        """
        Generate embeddings for documents in JSON file.
        
        Args:
            input_file: Path to input JSON file (array of documents)
            output_file: Path to output JSON file with embeddings
            text_field: Field name containing text to embed
            embedding_field: Field name for embedding output
            provider: Embedding provider ('openai' or 'sbert')
            model: Model name (provider-specific)
            api_key: API key for OpenAI
            batch_size: Batch size for processing
            context: Optional context for progress reporting
            
        Returns:
            Dict with status, count, and stats
        """
        logger.info("Starting embedding generation")
        logger.info(f"  Input: {input_file}")
        logger.info(f"  Output: {output_file}")
        logger.info(f"  Provider: {provider}")
        logger.info(f"  Model: {model or 'default'}")
        logger.info(f"  Text field: {text_field}")
        logger.info(f"  Embedding field: {embedding_field}")
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
                'output_file': output_file,
                'count': 0,
                'stats': {}
            }
        
        # Initialize embedder
        self._initialize_embedder(
            provider=provider,
            model=model,
            api_key=api_key
        )
        
        # Statistics
        stats = {
            'total_records': total_records,
            'embedded': 0,
            'skipped': 0,
            'failed': 0,
            'total_tokens': 0,
            'provider': self.provider,
            'model': self.model,
            'dimension': self.dimension
        }
        
        # Extract texts and track indices
        logger.info(f"Extracting texts from field '{text_field}'")
        texts_to_embed = []
        text_indices = []
        
        for i, record in enumerate(json_data):
            if not isinstance(record, dict):
                logger.warning(f"Record {i} is not a dict, skipping")
                stats['skipped'] += 1
                continue
            
            # Get text to embed
            text = record.get(text_field)
            
            if not text or not isinstance(text, str) or not text.strip():
                logger.warning(f"Record {i} has no valid text in field '{text_field}', skipping")
                stats['skipped'] += 1
                # Set empty embedding
                record[embedding_field] = None
                continue
            
            texts_to_embed.append(text)
            text_indices.append(i)
        
        if not texts_to_embed:
            logger.warning("No valid texts found to embed")
            return {
                'status': 'warning',
                'message': 'No valid texts to embed',
                'output_file': output_file,
                'count': 0,
                'stats': stats
            }
        
        logger.info(f"Found {len(texts_to_embed)} texts to embed")
        
        # Generate embeddings in batches
        logger.info(f"Generating embeddings in batches of {batch_size}")
        start_time = time.time()
        
        try:
            all_embeddings = []
            
            for batch_start in range(0, len(texts_to_embed), batch_size):
                batch_end = min(batch_start + batch_size, len(texts_to_embed))
                batch_texts = texts_to_embed[batch_start:batch_end]
                
                logger.debug(f"Processing batch {batch_start//batch_size + 1}: {len(batch_texts)} texts")
                
                try:
                    # Generate embeddings for batch
                    batch_embeddings = self.embedder.embed_batch(
                        batch_texts,
                        batch_size=batch_size,
                        show_progress=False
                    )
                    
                    all_embeddings.extend(batch_embeddings)
                    stats['embedded'] += len(batch_embeddings)
                    
                    # Report progress
                    if context:
                        context.report_progress(
                            batch_end,
                            len(texts_to_embed),
                            f"Generated {batch_end}/{len(texts_to_embed)} embeddings",
                            {'embedded': stats['embedded']}
                        )
                    
                    logger.debug(f"Batch complete: {len(batch_embeddings)} embeddings generated")
                    
                except Exception as e:
                    logger.error(f"Failed to generate embeddings for batch: {e}", exc_info=True)
                    stats['failed'] += len(batch_texts)
                    # Add None for failed embeddings
                    all_embeddings.extend([None] * len(batch_texts))
            
            # Assign embeddings back to records
            logger.info("Assigning embeddings to records")
            for idx, embedding in zip(text_indices, all_embeddings):
                json_data[idx][embedding_field] = embedding
            
            elapsed_time = time.time() - start_time
            embeddings_per_sec = stats['embedded'] / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(f"Embedding generation completed")
            logger.info(f"  Embedded: {stats['embedded']}")
            logger.info(f"  Skipped: {stats['skipped']}")
            logger.info(f"  Failed: {stats['failed']}")
            logger.info(f"  Time: {elapsed_time:.2f}s")
            logger.info(f"  Throughput: {embeddings_per_sec:.2f} embeddings/sec")
            
            stats['elapsed_time'] = elapsed_time
            stats['embeddings_per_sec'] = embeddings_per_sec
            
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}", exc_info=True)
            raise RuntimeError(f"Failed to generate embeddings: {e}")
        
        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save output JSON
        logger.info(f"Saving results to {output_file}")
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            file_size = output_path.stat().st_size
            logger.info(f"Output file saved: {file_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            logger.error(f"Failed to save output file: {e}", exc_info=True)
            raise IOError(f"Failed to save output file: {e}")
        
        if context:
            context.report_progress(
                total_records,
                total_records,
                "Embedding generation complete",
                stats
            )
        
        return {
            'status': 'success',
            'output_file': output_file,
            'count': stats['embedded'],
            'stats': stats
        }
    
    def get_embedding_dimension(self) -> int:
        """Get dimension of current embedder"""
        return self.dimension or 0