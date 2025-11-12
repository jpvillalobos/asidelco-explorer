"""
Workspace Manager - Creates and manages isolated working directories for pipeline runs
"""
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import json
import shutil
import logging

logger = logging.getLogger(__name__)


class WorkspaceManager:
    """Manages isolated working directories for pipeline runs"""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize workspace manager
        
        Args:
            base_dir: Base directory for all workspaces (defaults to project root)
        """
        if base_dir is None:
            # Default to project root
            base_dir = Path(__file__).parent.parent.parent / "workspaces"
        
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        self.current_workspace: Optional[Path] = None
        self.workspace_metadata: Dict[str, Any] = {}
    
    def create_workspace(
        self,
        name: Optional[str] = None,
        source_file: Optional[str] = None
    ) -> Path:
        """
        Create a new workspace directory
        
        Args:
            name: Optional custom name for workspace
            source_file: Optional source file path to base name on
            
        Returns:
            Path to created workspace directory
        """
        # Generate workspace name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if source_file:
            # Extract filename without extension
            source_path = Path(source_file)
            base_name = source_path.stem
            workspace_name = f"{base_name}_{timestamp}_workdir"
        elif name:
            workspace_name = f"{name}_{timestamp}_workdir"
        else:
            workspace_name = f"pipeline_{timestamp}_workdir"
        
        # Create workspace directory
        workspace_path = self.base_dir / workspace_name
        workspace_path.mkdir(parents=True, exist_ok=True)
        
        # Create standard subdirectories
        subdirs = {
            'data': workspace_path / 'data',
            'data_input': workspace_path / 'data' / 'input',
            'data_output': workspace_path / 'data' / 'output',
            'logs': workspace_path / 'logs',
            'temp': workspace_path / 'temp',
        }
        
        for subdir_name, subdir_path in subdirs.items():
            subdir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {subdir_path}")
        
        # Store metadata
        self.workspace_metadata = {
            'workspace_name': workspace_name,
            'workspace_path': str(workspace_path),
            'created_at': datetime.now().isoformat(),
            'source_file': str(source_file) if source_file else None,
            'custom_name': name,
            'subdirectories': {k: str(v) for k, v in subdirs.items()}
        }
        
        # Save metadata to workspace
        metadata_file = workspace_path / 'workspace_metadata.json'
        with open(metadata_file, 'w') as f:
            json.dump(self.workspace_metadata, f, indent=2)
        
        self.current_workspace = workspace_path
        logger.info(f"Created workspace: {workspace_path}")
        
        return workspace_path
    
    def get_path(self, subdir: str, filename: Optional[str] = None) -> Path:
        """
        Get path within current workspace
        
        Args:
            subdir: Subdirectory name (data_input, data_output, logs, temp)
            filename: Optional filename to append
            
        Returns:
            Full path within workspace
        """
        if not self.current_workspace:
            raise RuntimeError("No active workspace. Call create_workspace() first.")
        
        # Map common subdirectory names
        subdir_map = {
            'input': 'data/input',
            'data_input': 'data/input',
            'output': 'data/output',
            'data_output': 'data/output',
            'logs': 'logs',
            'temp': 'temp',
            'data': 'data'
        }
        
        mapped_subdir = subdir_map.get(subdir, subdir)
        path = self.current_workspace / mapped_subdir
        
        if filename:
            path = path / filename
        
        return path
    
    def copy_file_to_workspace(
        self,
        source_file: str,
        destination_subdir: str = 'data_input'
    ) -> Path:
        """
        Copy a file into the workspace
        
        Args:
            source_file: Path to source file
            destination_subdir: Subdirectory within workspace
            
        Returns:
            Path to copied file in workspace
        """
        if not self.current_workspace:
            raise RuntimeError("No active workspace")
        
        source_path = Path(source_file)
        
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        # Determine destination
        dest_dir = self.get_path(destination_subdir)
        dest_file = dest_dir / source_path.name
        
        # Copy file
        shutil.copy2(source_path, dest_file)
        logger.info(f"Copied {source_path.name} to workspace: {dest_file}")
        
        return dest_file
    
    def get_log_file(self, log_name: str) -> Path:
        """
        Get path to log file in workspace
        
        Args:
            log_name: Name of log file (e.g., 'pipeline.log', 'errors.log')
            
        Returns:
            Path to log file
        """
        return self.get_path('logs', log_name)
    
    def get_temp_file(self, filename: str) -> Path:
        """Get path to temporary file in workspace"""
        return self.get_path('temp', filename)
    
    def list_workspaces(self) -> list:
        """List all available workspaces"""
        workspaces = []
        
        for item in self.base_dir.iterdir():
            if item.is_dir() and item.name.endswith('_workdir'):
                metadata_file = item / 'workspace_metadata.json'
                
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    workspaces.append(metadata)
                else:
                    # Workspace without metadata
                    workspaces.append({
                        'workspace_name': item.name,
                        'workspace_path': str(item),
                        'created_at': datetime.fromtimestamp(item.stat().st_ctime).isoformat()
                    })
        
        # Sort by creation time (newest first)
        workspaces.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return workspaces
    
    def load_workspace(self, workspace_path: str) -> bool:
        """
        Load an existing workspace
        
        Args:
            workspace_path: Path to workspace directory
            
        Returns:
            True if successful
        """
        ws_path = Path(workspace_path)
        
        if not ws_path.exists() or not ws_path.is_dir():
            logger.error(f"Workspace not found: {workspace_path}")
            return False
        
        # Load metadata if available
        metadata_file = ws_path / 'workspace_metadata.json'
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                self.workspace_metadata = json.load(f)
        
        self.current_workspace = ws_path
        logger.info(f"Loaded workspace: {workspace_path}")
        
        return True
    
    def cleanup_workspace(self, workspace_path: Optional[str] = None) -> bool:
        """
        Delete a workspace and all its contents
        
        Args:
            workspace_path: Path to workspace (uses current if None)
            
        Returns:
            True if successful
        """
        if workspace_path:
            ws_path = Path(workspace_path)
        elif self.current_workspace:
            ws_path = self.current_workspace
        else:
            logger.error("No workspace specified for cleanup")
            return False
        
        if ws_path.exists():
            shutil.rmtree(ws_path)
            logger.info(f"Deleted workspace: {ws_path}")
            
            if self.current_workspace == ws_path:
                self.current_workspace = None
                self.workspace_metadata = {}
            
            return True
        
        return False
    
    def get_workspace_summary(self) -> Dict[str, Any]:
        """Get summary of current workspace"""
        if not self.current_workspace:
            return {"error": "No active workspace"}
        
        summary = {
            "workspace_path": str(self.current_workspace),
            "workspace_name": self.current_workspace.name,
            "created_at": self.workspace_metadata.get('created_at'),
            "source_file": self.workspace_metadata.get('source_file'),
            "directories": {},
            "files": {}
        }
        
        # Count files in each directory
        for subdir in ['data/input', 'data/output', 'logs', 'temp']:
            subdir_path = self.current_workspace / subdir
            if subdir_path.exists():
                files = list(subdir_path.rglob('*'))
                file_count = len([f for f in files if f.is_file()])
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                
                summary['directories'][subdir] = {
                    'file_count': file_count,
                    'total_size_mb': round(total_size / (1024 * 1024), 2)
                }
        
        return summary
    
    def export_workspace_archive(self, output_path: Optional[str] = None) -> Path:
        """
        Create a zip archive of the workspace
        
        Args:
            output_path: Optional path for archive (defaults to workspace parent)
            
        Returns:
            Path to created archive
        """
        if not self.current_workspace:
            raise RuntimeError("No active workspace")
        
        if output_path:
            archive_path = Path(output_path)
        else:
            archive_path = self.base_dir / f"{self.current_workspace.name}.zip"
        
        # Create archive
        shutil.make_archive(
            str(archive_path.with_suffix('')),
            'zip',
            self.current_workspace
        )
        
        logger.info(f"Created workspace archive: {archive_path}")
        return archive_path