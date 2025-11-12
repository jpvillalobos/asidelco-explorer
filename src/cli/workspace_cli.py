"""
CLI for Workspace Management
"""
import click
from pathlib import Path
from src.pipeline.workspace import WorkspaceManager


@click.group()
def workspace():
    """Manage pipeline workspaces"""
    pass


@workspace.command()
def list():
    """List all available workspaces"""
    manager = WorkspaceManager()
    workspaces = manager.list_workspaces()
    
    if not workspaces:
        click.echo("No workspaces found.")
        return
    
    click.echo(f"\nFound {len(workspaces)} workspaces:\n")
    
    for ws in workspaces:
        click.echo(f"  📁 {ws['workspace_name']}")
        click.echo(f"     Created: {ws.get('created_at', 'Unknown')}")
        click.echo(f"     Path: {ws['workspace_path']}")
        
        if ws.get('source_file'):
            click.echo(f"     Source: {ws['source_file']}")
        
        click.echo()


@workspace.command()
@click.argument('workspace_path')
def cleanup(workspace_path):
    """Delete a workspace"""
    manager = WorkspaceManager()
    
    if click.confirm(f"Are you sure you want to delete '{workspace_path}'?"):
        if manager.cleanup_workspace(workspace_path):
            click.echo(f"✅ Deleted workspace: {workspace_path}")
        else:
            click.echo(f"❌ Failed to delete workspace: {workspace_path}")


@workspace.command()
@click.argument('workspace_path')
@click.option('--output', '-o', help='Output archive path')
def export(workspace_path, output):
    """Export workspace as archive"""
    manager = WorkspaceManager()
    
    if not manager.load_workspace(workspace_path):
        click.echo(f"❌ Workspace not found: {workspace_path}")
        return
    
    archive_path = manager.export_workspace_archive(output)
    click.echo(f"✅ Exported to: {archive_path}")


@workspace.command()
@click.argument('workspace_path')
def info(workspace_path):
    """Show workspace information"""
    manager = WorkspaceManager()
    
    if not manager.load_workspace(workspace_path):
        click.echo(f"❌ Workspace not found: {workspace_path}")
        return
    
    summary = manager.get_workspace_summary()
    
    click.echo(f"\n📁 Workspace: {summary['workspace_name']}\n")
    click.echo(f"Path: {summary['workspace_path']}")
    click.echo(f"Created: {summary.get('created_at', 'Unknown')}")
    
    if summary.get('source_file'):
        click.echo(f"Source: {summary['source_file']}")
    
    click.echo("\nDirectories:")
    for dir_name, dir_info in summary.get('directories', {}).items():
        click.echo(f"  {dir_name}: {dir_info['file_count']} files ({dir_info['total_size_mb']} MB)")


if __name__ == '__main__':
    workspace()