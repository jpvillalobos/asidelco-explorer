"""
Pipeline execution engine.
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from .config import PipelineConfig, StageConfig, StepConfig, load_pipeline_config, resolve_variables
from .registry import get_registry
from .progress import ProgressTracker
from services.storage_service import WorkspaceManager


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