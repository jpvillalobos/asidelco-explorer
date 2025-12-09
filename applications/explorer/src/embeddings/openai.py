#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Embed CFIA project JSON records with OpenAI text-embedding-3-small.
If a file already holds a non-empty vector for this model, the script skips it.

All configuration is embedded in the code (no CLI arguments).
"""

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..utils.openai_client import get_openai_client

# ────────────────────────── Configuration ────────────────────────── #

OPENAI_MODEL_NAME = "text-embedding-3-small"
OVERWRITE_EXISTING = False          # True → regenerate even if vector exists

INPUT_DIR = Path(
    "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/enhanced"
)
LOG_FILE = Path(
    "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/logs/embedder-2.log"
)

SUMMARY_FIELD_PATH = ("resumen", "value")   # nested keys to summary
OPENAI_TIMEOUT = 60                         # seconds

# ──────────────────────────── Logging ────────────────────────────── #

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),  # append
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ────────────────────────── OpenAI client ────────────────────────── #

try:
    client = OpenAI(timeout=OPENAI_TIMEOUT)
except Exception as exc:  # pylint: disable=broad-exception-caught
    logger.error("Failed to create OpenAI client: %s", exc)
    sys.exit(1)

# ───────────────────────── Helper functions ──────────────────────── #


def safe_model_key(model_name: str) -> str:
    """Replace '/' so the model name can be used as a JSON key."""
    return model_name.replace("/", "-")


def fmt_elapsed(start: float) -> str:
    """Return HH:MM:SS since `start` (perf_counter)."""
    seconds = int(time.perf_counter() - start)
    hrs, seconds = divmod(seconds, 3600)
    mins, secs = divmod(seconds, 60)
    return f"{hrs:02d}:{mins:02d}:{secs:02d}"


def load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def save_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def get_summary(doc: Dict[str, Any]) -> str:
    node: Any = doc
    for key in SUMMARY_FIELD_PATH:
        if key not in node:
            raise KeyError(f"Missing '{'.'.join(SUMMARY_FIELD_PATH)}'")
        node = node[key]
    if not isinstance(node, str):
        raise TypeError("Summary field is not a string")
    return node


# class OpenAIEmbeddings:
#     def __init__(
#         self,
#         api_key: Optional[str] = None,
#         model: str = "text-embedding-3-small",
#         base_url: Optional[str] = None,
#         organization: Optional[str] = None,
#         **kwargs,
#     ):
#         self.api_key = api_key
#         self.model = model
#         self.base_url = base_url
#         self.organization = organization
#         self._client = None
#         self.dimension = 1536  # for text-embedding-3-small

#     def _ensure_client(self):
#         if self._client is None:
#             self._client = get_openai_client(
#                 api_key=self.api_key,
#                 base_url=self.base_url,
#                 organization=self.organization,
#             )

#     def embed(self, text: str, **kwargs) -> List[float]:
#         self._ensure_client()
#         resp = self._client.embeddings.create(model=self.model, input=text)
#         return resp.data[0].embedding

#     def embed_batch(self, texts: List[str], batch_size: int = 32, show_progress: bool = False) -> List[List[float]]:
#         self._ensure_client()
#         out: List[List[float]] = []
#         rng = range(0, len(texts), batch_size)
#         if show_progress:
#             try:
#                 from tqdm import tqdm
#                 rng = tqdm(rng)
#             except Exception:
#                 pass
#         for i in rng:
#             chunk = texts[i:i+batch_size]
#             resp = self._client.embeddings.create(model=self.model, input=chunk)
#             out.extend([d.embedding for d in resp.data])
#         return out

# @retry(wait=wait_fixed(2), stop=stop_after_attempt(3), reraise=True)
# def get_embedding(text: str, model: str) -> List[float]:
#     """Fetch the embedding from OpenAI."""
#     resp = client.embeddings.create(model=model, input=text)
#     return resp.data[0].embedding  # type: ignore[index]

# ───────────────────────────── Main ─────────────────────────────── #

model_key = safe_model_key(OPENAI_MODEL_NAME)
json_files = sorted(p for p in INPUT_DIR.glob("*.json") if p.is_file())
total = len(json_files)

if total == 0:
    logger.warning("No JSON files found in %s", INPUT_DIR)
    sys.exit(0)

start_time = time.perf_counter()
logger.info("Embedding %d files with model '%s' …", total, OPENAI_MODEL_NAME)

for idx, path in enumerate(json_files, start=1):
    try:
        doc = load_json(path)

        # Ensure embeddings container exists
        embeddings = doc.get("embeddings")
        if embeddings is None:
            embeddings = {}
            doc["embeddings"] = embeddings

        # Immediate skip if vector already present and non-empty
        already = (
            model_key in embeddings
            and isinstance(embeddings[model_key], list)
            and len(embeddings[model_key]) > 0
        )
        if already and not OVERWRITE_EXISTING:
            logger.info(
                "[%d/%d] (%s) %s → skip (already has '%s')",
                idx,
                total,
                fmt_elapsed(start_time),
                path.name,
                model_key,
            )
            continue

        # Migrate legacy single-field format if still present
        if "embedding" in doc:
            legacy_vec = doc.pop("embedding")
            if isinstance(legacy_vec, dict) and "value" in legacy_vec:
                embeddings.setdefault("all-MiniLM-L6-v2", legacy_vec["value"])

        summary = get_summary(doc)
        vector = get_embedding(summary, OPENAI_MODEL_NAME)
        embeddings[model_key] = vector
        save_json(path, doc)

        logger.info(
            "[%d/%d] (%s) %s → embedded (%s)",
            idx,
            total,
            fmt_elapsed(start_time),
            path.name,
            model_key,
        )

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception(
            "[%d/%d] (%s) %s → FAILED: %s",
            idx,
            total,
            fmt_elapsed(start_time),
            path.name,
            exc,
        )

logger.info("Embedding pass finished in %s.", fmt_elapsed(start_time))
