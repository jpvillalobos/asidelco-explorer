"""Pipeline configuration loader"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import os
import yaml

@dataclass
class StepConfig:
    """Step configuration"""
    name: str
    title: str
    args: Dict[str, Any] = field(default_factory=dict)

@dataclass
class StageConfig:
    """Stage configuration"""
    id: str
    title: str
    description: str = ""
    steps: List[StepConfig] = field(default_factory=list)

@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    name: str
    version: str
    description: str
    workspace_root: str
    stages: List[StageConfig]

def resolve_variables(config: Dict[str, Any], variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Resolve variables in configuration.
    
    Args:
        config: Configuration dictionary
        variables: Optional variables to replace
    
    Returns:
        Configuration with resolved variables
    """
    if variables is None:
        variables = {}
    
    # Simple variable resolution - can be enhanced
    resolved = config.copy()
    
    # Add environment variables
    variables.update(os.environ)
    
    return resolved

def load_base_pipeline_config() -> Tuple[Optional[Dict[str, Any]], Optional[Path], Optional[str]]:
    """
    Load the base pipeline configuration from YAML.
    
    Returns:
        Tuple of (config_dict, config_path, error_message)
    """
    # Priority order:
    # 1. Environment variable BASE_PIPELINE_YAML
    # 2. Default: src/pipeline/pipeline_config.yaml
    
    config_path = None
    
    # Check environment variable
    env_path = os.environ.get("BASE_PIPELINE_YAML")
    if env_path:
        config_path = Path(env_path)
        if not config_path.exists():
            return None, config_path, f"Config file not found: {config_path}"
    
    # Use default location
    if not config_path:
        # Get the directory where this config.py file is located
        pipeline_dir = Path(__file__).parent
        config_path = pipeline_dir / "pipeline_config.yaml"
        
        if not config_path.exists():
            return None, config_path, f"Default config not found: {config_path}"
    
    # Load YAML
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return config_data, config_path, None
    
    except Exception as e:
        return None, config_path, f"Error loading config: {e}"

def load_pipeline_config(config_path: Optional[Path] = None) -> Optional[PipelineConfig]:
    """
    Load and parse pipeline configuration into PipelineConfig object.
    
    Args:
        config_path: Optional path to config file
    
    Returns:
        PipelineConfig object or None if loading fails
    """
    if config_path:
        try:
            with open(config_path, 'r') as f:
                config_dict = yaml.safe_load(f)
        except Exception:
            return None
    else:
        config_dict, _, error = load_base_pipeline_config()
        if error or not config_dict:
            return None
    
    pipeline_data = config_dict.get("pipeline", {})
    
    # Parse stages
    stages = []
    for stage_data in pipeline_data.get("stages", []):
        # Parse steps
        steps = []
        for step_data in stage_data.get("steps", []):
            step = StepConfig(
                name=step_data.get("name", ""),
                title=step_data.get("title", ""),
                args=step_data.get("args", {})
            )
            steps.append(step)
        
        stage = StageConfig(
            id=stage_data.get("id", ""),
            title=stage_data.get("title", ""),
            description=stage_data.get("description", ""),
            steps=steps
        )
        stages.append(stage)
    
    return PipelineConfig(
        name=pipeline_data.get("name", "unknown"),
        version=str(config_dict.get("version", "1.0")),
        description=pipeline_data.get("description", ""),
        workspace_root=pipeline_data.get("workspace_root", "workspaces"),
        stages=stages
    )

def get_pipeline_config() -> Optional[PipelineConfig]:
    """
    Load and parse pipeline configuration into PipelineConfig object.
    
    Returns:
        PipelineConfig object or None if loading fails
    """
    return load_pipeline_config()