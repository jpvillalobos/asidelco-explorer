"""Pipeline configuration loader and parser."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import os
import yaml

@dataclass
class StepConfig:
    """Configuration for a pipeline step."""
    name: str
    title: str
    args: Dict[str, Any]


@dataclass
class StageConfig:
    """Configuration for a pipeline stage."""
    id: str
    title: str
    steps: List[StepConfig]


@dataclass
class PipelineConfig:
    """Configuration for the entire pipeline."""
    name: str
    description: str
    workspace_root: str
    version: int
    stages: List[StageConfig]


def load_pipeline_config(config_path: str) -> PipelineConfig:
    """Load pipeline configuration from YAML file."""
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
    
    pipeline_data = data['pipeline']
    
    stages = []
    for stage_data in pipeline_data['stages']:
        steps = []
        for step_data in stage_data['steps']:
            steps.append(StepConfig(
                name=step_data['name'],
                title=step_data['title'],
                args=step_data.get('args', {})
            ))
        stages.append(StageConfig(
            id=stage_data['id'],
            title=stage_data['title'],
            steps=steps
        ))
    
    return PipelineConfig(
        name=pipeline_data['name'],
        description=pipeline_data['description'],
        workspace_root=pipeline_data['workspace_root'],
        version=data['version'],
        stages=stages
    )


def resolve_variables(value: Any, workspace_path: str) -> Any:
    """Resolve ${workspace} variables in configuration values."""
    if isinstance(value, str):
        return value.replace('${workspace}', workspace_path)
    elif isinstance(value, dict):
        return {k: resolve_variables(v, workspace_path) for k, v in value.items()}
    elif isinstance(value, list):
        return [resolve_variables(v, workspace_path) for v in value]
    return value


def load_base_pipeline_config(explicit_path: Optional[str] = None) -> Tuple[Optional[dict], Optional[Path], Optional[str]]:
    """
    Load a base pipeline.yaml used only to render UI defaults.
    Returns (config_dict, path_used, error_message). Never triggers execution.
    """
    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    env_path = os.environ.get("BASE_PIPELINE_YAML")
    if env_path:
        candidates.append(Path(env_path))

    # Project-relative fallbacks
    here = Path(__file__).resolve()
    src_dir = here.parents[1]           # .../src
    project_root = src_dir.parent
    candidates += [
        project_root / "pipeline_config.yaml",
        project_root / "pipeline.yaml",
        project_root / "base-pipeline.yaml",
        src_dir / "pipeline" / "pipeline.yaml",
        src_dir / "pipeline" / "base_pipeline.yaml",
    ]

    for p in candidates:
        if not p.is_file():
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data, p, None
        except Exception as e:
            return None, p, f"Failed to read YAML at {p}: {e}"
    return None, None, "No base pipeline.yaml found. Set BASE_PIPELINE_YAML or add one to the repo."


def save_pipeline_config_to_workspace(workspace_path: str | Path, config: dict, filename: str = "pipeline.yaml") -> Path:
    """
    Save a pipeline config into the given workspace path without executing anything.
    """
    ws = Path(workspace_path)
    ws.mkdir(parents=True, exist_ok=True)
    out = ws / filename
    with out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config or {}, f, sort_keys=False, indent=2, allow_unicode=True)
    return out