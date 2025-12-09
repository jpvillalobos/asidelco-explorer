"""
Asidelco Explorer Package
Initialize logging and create necessary directories
"""
from pathlib import Path
import logging

# Create necessary directories
project_root = Path(__file__).parent.parent
logs_dir = project_root / "logs"
logs_dir.mkdir(exist_ok=True)

# Create data directories
data_dir = project_root / "data"
(data_dir / "input").mkdir(parents=True, exist_ok=True)
(data_dir / "output").mkdir(parents=True, exist_ok=True)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / "application.log")
    ]
)

logger = logging.getLogger(__name__)
logger.info("Asidelco Explorer initialized")