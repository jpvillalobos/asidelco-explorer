"""
Pipeline execution engine.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
import logging

from .config import PipelineConfig, StageConfig, StepConfig, load_pipeline_config, resolve_variables
from .registry import get_registry
from .progress import ProgressTracker
from pipeline.workspace import WorkspaceManager

logger = logging.getLogger(__name__)


class Pipeline:
    """Simple Pipeline interface for step-by-step execution."""

    def __init__(self, workspace_dir: str):
        self.workspace_dir = Path(workspace_dir)
        # Load the existing workspace instead of creating a new one
        self.workspace = WorkspaceManager(base_dir=self.workspace_dir.parent)
        self.workspace.load_workspace(str(self.workspace_dir))
        self.registry = get_registry()
        # Set workspace directory on registry for step-specific logging
        self.registry.set_workspace_dir(str(self.workspace_dir))
        self.steps = []

        # Setup logging to workspace logs directory
        self._setup_logging()

    def _setup_logging(self):
        """Setup logging to workspace logs directory."""
        logs_dir = self.workspace_dir / 'logs'
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Create log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = logs_dir / f"pipeline_{timestamp}.log"

        # Configure file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)

        # Add handler to root logger and pipeline logger
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        root_logger.setLevel(logging.INFO)

        logger.info(f"Pipeline initialized for workspace: {self.workspace_dir}")
        logger.info(f"Logging to: {log_file}")

    def add_step(self, step_type, **kwargs):
        """Add a step to the pipeline."""
        self.steps.append((step_type, kwargs))
        logger.debug(f"Added step: {step_type} with args: {kwargs}")

    def run(self):
        """Execute all added steps."""
        logger.info(f"Starting pipeline execution with {len(self.steps)} step(s)")

        # Resolve paths relative to workspace
        for i, (step_type, kwargs) in enumerate(self.steps, 1):
            # Get step name - handle both enum and string types
            step_name = step_type.value if hasattr(step_type, 'value') else step_type
            logger.info(f"Step {i}/{len(self.steps)}: {step_name}")

            # Convert relative paths to absolute paths within workspace
            resolved_kwargs = {}
            for key, value in kwargs.items():
                if isinstance(value, str) and ('file' in key.lower() or 'path' in key.lower()):
                    # Check if it's a relative path
                    if not Path(value).is_absolute():
                        # If it's a simple filename (no subdirectory), place it appropriately
                        value_path = Path(value)
                        if len(value_path.parts) == 1:  # Just a filename
                            # Output files go to data/output, input files stay as-is
                            if 'output' in key.lower():
                                resolved_kwargs[key] = str(self.workspace_dir / 'data' / 'output' / value)
                            else:
                                resolved_kwargs[key] = str(self.workspace_dir / value)
                        else:
                            # Path includes subdirectories, use as-is
                            resolved_kwargs[key] = str(self.workspace_dir / value)
                    else:
                        resolved_kwargs[key] = value
                else:
                    resolved_kwargs[key] = value

            logger.debug(f"Resolved arguments: {resolved_kwargs}")

            try:
                step_func = self.registry.get(step_name)
                logger.info(f"Executing step: {step_name}")
                result = step_func(**resolved_kwargs)
                logger.info(f"Step {step_name} completed successfully")
                logger.debug(f"Result: {result}")
            except Exception as e:
                logger.error(f"Step {step_name} failed: {str(e)}", exc_info=True)
                raise

        logger.info("Pipeline execution completed successfully")
        return result


class PipelineExecutor:
    """Executes pipeline based on configuration."""

    def __init__(self, config: PipelineConfig, workspace: WorkspaceManager):
        self.config = config
        self.workspace = workspace
        self.registry = get_registry()
        self.progress = ProgressTracker()
        self._results: Dict[str, Any] = {}
    
    @classmethod
    def from_config_file(cls, config_path: str, workspace_name: Optional[str] = None):
        """Create executor from configuration file."""
        config = load_pipeline_config(config_path)
        
        if workspace_name is None:
            workspace_name = f"{config.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        workspace = WorkspaceManager(
            name=workspace_name,
            root_dir=config.workspace_root
        )
        workspace.initialize()
        
        return cls(config, workspace)
    
    def execute(self, start_stage: Optional[str] = None, end_stage: Optional[str] = None) -> Dict[str, Any]:
        """Execute the pipeline."""
        self.progress.start(len(self.config.stages))
        
        # Filter stages if start/end specified
        stages_to_run = self._filter_stages(start_stage, end_stage)
        
        for stage in stages_to_run:
            self._execute_stage(stage)
        
        self.progress.complete()
        
        return {
            'status': 'success',
            'workspace': self.workspace.path,
            'results': self._results
        }
    
    def _filter_stages(self, start_stage: Optional[str], end_stage: Optional[str]) -> List[StageConfig]:
        """Filter stages based on start/end stage IDs."""
        stages = self.config.stages
        
        if start_stage:
            start_idx = next((i for i, s in enumerate(stages) if s.id == start_stage), 0)
            stages = stages[start_idx:]
        
        if end_stage:
            end_idx = next((i for i, s in enumerate(stages) if s.id == end_stage), len(stages) - 1)
            stages = stages[:end_idx + 1]
        
        return stages
    
    def _execute_stage(self, stage: StageConfig):
        """Execute a single stage."""
        self.progress.update_stage(stage.id, stage.title)
        stage_results = []
        
        for i, step in enumerate(stage.steps):
            step_result = self._execute_step(step, stage.id, i + 1, len(stage.steps))
            stage_results.append(step_result)
        
        self._results[stage.id] = stage_results
    
    def _execute_step(self, step: StepConfig, stage_id: str, step_num: int, total_steps: int) -> Dict[str, Any]:
        """Execute a single step."""
        self.progress.update_step(step.title, step_num, total_steps)
        
        # Resolve workspace variables in args
        resolved_args = resolve_variables(step.args, str(self.workspace.path))
        
        try:
            # Get step function from registry
            step_func = self.registry.get(step.name)
            
            # Execute step
            result = step_func(**resolved_args)
            
            return {
                'step': step.name,
                'title': step.title,
                'status': 'success',
                'result': result
            }
        
        except Exception as e:
            error_result = {
                'step': step.name,
                'title': step.title,
                'status': 'error',
                'error': str(e)
            }
            self.progress.error(f"Error in {step.title}: {str(e)}")
            return error_result
    
    def get_stage_config(self, stage_id: str) -> Optional[StageConfig]:
        """Get configuration for a specific stage."""
        return next((s for s in self.config.stages if s.id == stage_id), None)
    
    def get_step_config(self, stage_id: str, step_name: str) -> Optional[StepConfig]:
        """Get configuration for a specific step."""
        stage = self.get_stage_config(stage_id)
        if stage:
            return next((s for s in stage.steps if s.name == step_name), None)
        return None