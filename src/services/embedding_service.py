"""
Embedding Service
"""
from typing import List, Dict, Any, Optional, Union
import logging
import json
from pathlib import Path

from embeddings.sbert import generate_embeddings
from embeddings.openai import generate_openai_embeddings

class EmbeddingService:
    """Service for generating text embeddings (OpenAI only)"""

    def __init__(self, provider: str = "openai", model: Optional[str] = None, api_key: Optional[str] = None, **kwargs):
        provider = (provider or "openai").lower()
        if provider != "openai":
            raise RuntimeError("Only OpenAI embeddings are supported currently. Use provider='openai'.")
        self.embedder = generate_openai_embeddings(api_key=api_key, model=model or "text-embedding-3-small", **kwargs)

    def generate_embedding(self, text: str, **kwargs) -> List[float]:
        return self.embedder.embed(text, **kwargs)

    def generate_embeddings(self, texts: List[str], batch_size: int = 32, show_progress: bool = False) -> List[List[float]]:
        return self.embedder.embed_batch(texts, batch_size=batch_size, show_progress=show_progress)

    def generate_documents_embeddings(self, documents: List[Dict], text_field: str = "text", embedding_field: str = "embedding") -> List[Dict]:
        texts = [doc.get(text_field, "") for doc in documents]
        embeddings = self.generate_embeddings(texts)
        for doc, emb in zip(documents, embeddings):
            doc[embedding_field] = emb
        return documents

    def get_embedding_dimension(self) -> int:
        return getattr(self.embedder, "dimension", None) or 0

def load_data(input_file):
    """Load data from file."""
    with open(input_file, 'r') as f:
        return json.load(f)