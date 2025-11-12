"""
Parser Service
"""
from typing import Dict, Any, Optional, List, Union
import logging
from pathlib import Path

from . import config
from . import errors
from . import models
from etl.extract.html_parser import HTMLParser
from etl.extract.html_to_json import html_to_json

logger = logging.getLogger(__name__)


class ParserService:
    def __init__(self, config: config.Config):
        self.config = config

    def parse(self, file_path: Path) -> models.Document:
        """
        Parse a file and return a Document object.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_type = file_path.suffix.lower()
        if file_type not in self.config.supported_file_types:
            raise ValueError(f"Unsupported file type: {file_type}")

        return self._parse_file(file_path)

    def _parse_file(self, file_path: Path) -> models.Document:
        """
        Parse a file and return a Document object.
        """
        file_type = file_path.suffix.lower()
        if file_type == ".json":
            return self._parse_json(file_path)
        elif file_type == ".xml":
            return self._parse_xml(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def _parse_json(self, file_path: Path) -> models.Document:
        """
        Parse a JSON file and return a Document object.
        """
        with open(file_path, "r") as file:
            data = json.load(file)
        return models.Document(data)

    def _parse_xml(self, file_path: Path) -> models.Document:
        """
        Parse an XML file and return a Document object.
        """
        with open(file_path, "r") as file:
            data = file.read()
        return models.Document(data)