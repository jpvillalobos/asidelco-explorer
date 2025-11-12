"""CLI for pipeline execution."""
import click
from pathlib import Path
import sys

# Add parent directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.pipeline import PipelineExecutor
from pipeline.config import load_pipeline_config


@click.group()
def cli():
    """ASIDELCO Explorer Pipeline CLI."""
    pass


@cli.command()
@click.option('--config', default='pipeline_config.yaml', help='Pipeline configuration file')
@click.option('--workspace', help='Workspace name')
@click.option('--start-stage', help='Start from this stage')
@click.option('--end-stage', help='End at this stage')
def run(config, workspace, start_stage, end_stage):
    """Execute the pipeline."""
    # Look in project root (3 levels up from src/cli/)
    config_path = Path(__file__).parent.parent.parent / config
    
    if not config_path.exists():
        # Try current directory
        config_path = Path(config)
        if not config_path.exists():
            click.echo(f"Error: Configuration file not found: {config}", err=True)
            return
    
    click.echo(f"Loading configuration from {config_path}")
    executor = PipelineExecutor.from_config_file(str(config_path), workspace)
    
    click.echo(f"Executing pipeline: {executor.config.name}")
    click.echo(f"Workspace: {executor.workspace.path}")
    
    result = executor.execute(start_stage=start_stage, end_stage=end_stage)
    
    if result['status'] == 'success':
        click.echo("✓ Pipeline executed successfully!")
        click.echo(f"Results saved to: {result['workspace']}")
    else:
        click.echo("✗ Pipeline execution failed!", err=True)


@cli.command()
@click.option('--config', default='pipeline_config.yaml', help='Pipeline configuration file')
def stages(config):
    """List all pipeline stages."""
    config_path = Path(__file__).parent.parent.parent / config
    if not config_path.exists():
        config_path = Path(config)
    
    config_obj = load_pipeline_config(str(config_path))
    
    click.echo(f"Pipeline: {config_obj.name}")
    click.echo(f"Stages: {len(config_obj.stages)}")
    click.echo()
    
    for idx, stage in enumerate(config_obj.stages, 1):
        click.echo(f"{idx}. {stage.title} ({stage.id})")
        click.echo(f"   Steps: {len(stage.steps)}")
        for step in stage.steps:
            click.echo(f"   - {step.title} [{step.name}]")
        click.echo()


@cli.command()
@click.option('--config', default='pipeline_config.yaml', help='Pipeline configuration file')
@click.argument('stage_id')
def stage_info(config, stage_id):
    """Show detailed information about a stage."""
    config_path = Path(__file__).parent.parent.parent / config
    if not config_path.exists():
        config_path = Path(config)
    
    config_obj = load_pipeline_config(str(config_path))
    stage = next((s for s in config_obj.stages if s.id == stage_id), None)
    
    if not stage:
        click.echo(f"Error: Stage '{stage_id}' not found", err=True)
        return
    
    click.echo(f"Stage: {stage.title}")
    click.echo(f"ID: {stage.id}")
    click.echo(f"Steps: {len(stage.steps)}")
    click.echo()
    
    for idx, step in enumerate(stage.steps, 1):
        click.echo(f"{idx}. {step.title}")
        click.echo(f"   Name: {step.name}")
        click.echo(f"   Arguments:")
        for key, value in step.args.items():
            click.echo(f"     - {key}: {value}")
        click.echo()


if __name__ == '__main__':
    cli()