from __future__ import annotations
"""
Streamlit UI for Pipeline Management with Workspace Support
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import time
from enum import Enum

import streamlit as st

# Charts removed - using simple progress bars only

# Must be the first Streamlit command
if "_page_configured" not in st.session_state:
    try:
        st.set_page_config(
            page_title="Asidelco Explorer",
            page_icon="⚙️",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        st.session_state["_page_configured"] = True
    except Exception:
        pass

# Ensure /src is on sys.path
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline.workspace import WorkspaceManager
from pipeline.config import load_base_pipeline_config
from pipeline.progress import ProgressTracker, ProgressEvent, ExecutionState
# from pipeline.pipeline import Pipeline
# from pipeline.steps import StepType

# Update CSS for better font sizes
st.markdown("""
<style>
    .main-header {
        font-size: 1.75rem;
        font-weight: 600;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .section-header {
        font-size: 1.25rem;
        font-weight: 500;
        color: #2c3e50;
        margin-top: 1rem;
        margin-bottom: 0.75rem;
    }
    .workspace-card {
        background-color: #f8f9fa;
        padding: 0.75rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
        margin-bottom: 0.75rem;
    }
    .step-card {
        background-color: #ffffff;
        padding: 0.75rem;
        border-radius: 0.5rem;
        border: 1px solid #e9ecef;
        margin-bottom: 0.5rem;
    }
    .status-badge {
        padding: 0.2rem 0.6rem;
        border-radius: 0.75rem;
        font-size: 0.75rem;
        font-weight: 500;
    }
    .status-ready {
        background-color: #d4edda;
        color: #155724;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 0.75rem;
        border-radius: 0.5rem;
        color: white;
    }
    /* Fix for metric labels */
    [data-testid="stMetricLabel"] {
        font-size: 0.875rem;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.25rem;
    }
    /* Tab text sizing */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 0.875rem;
    }
    /* Expander header */
    .streamlit-expanderHeader {
        font-size: 0.875rem;
    }
</style>
""", unsafe_allow_html=True)

# Update initialize_session_state() to add new workspace flag
def initialize_session_state():
    """Initialize session state variables"""
    if 'workspace_manager' not in st.session_state:
        st.session_state.workspace_manager = WorkspaceManager()
    if 'current_workspace' not in st.session_state:
        st.session_state.current_workspace = None
    if 'workspace_needs_creation' not in st.session_state:
        st.session_state.workspace_needs_creation = True
    if 'files_loaded' not in st.session_state:
        st.session_state.files_loaded = 0
    if 'steps_selected' not in st.session_state:
        st.session_state.steps_selected = 0
    if 'active_stage' not in st.session_state:
        st.session_state.active_stage = 0
    if 'progress_tracker' not in st.session_state:
        st.session_state.progress_tracker = ProgressTracker()
    if 'pipeline_running' not in st.session_state:
        st.session_state.pipeline_running = False
    if 'stage_progress' not in st.session_state:
        st.session_state.stage_progress = {}
    if 'step_status' not in st.session_state:
        st.session_state.step_status = {}
    if 'step_enabled' not in st.session_state:
        st.session_state.step_enabled = {}
    if 'step_args' not in st.session_state:
        st.session_state.step_args = {}
    # New: runtime logs, history and stop flag
    if 'pipeline_logs' not in st.session_state:
        st.session_state.pipeline_logs = []
    if 'execution_history' not in st.session_state:
        # list of runs per workspace
        st.session_state.execution_history = []
    if 'stop_requested' not in st.session_state:
        st.session_state.stop_requested = False


def get_workspace_stats(ws_path: str) -> Dict[str, Any]:
    """Get statistics for a workspace"""
    ws = Path(ws_path)
    stats = {
        "input_files": 0,
        "input_size_mb": 0.0,
        "output_files": 0,
        "output_size_mb": 0.0,
        "log_files": 0,
        "log_size_mb": 0.0,
        "temp_files": 0,
        "temp_size_mb": 0.0,
    }
    
    try:
        # Count input files
        input_dir = ws / "data" / "input"
        if input_dir.exists():
            for f in input_dir.rglob("*"):
                if f.is_file():
                    stats["input_files"] += 1
                    stats["input_size_mb"] += f.stat().st_size / (1024 * 1024)
        
        # Count output files
        output_dir = ws / "data" / "output"
        if output_dir.exists():
            for f in output_dir.rglob("*"):
                if f.is_file():
                    stats["output_files"] += 1
                    stats["output_size_mb"] += f.stat().st_size / (1024 * 1024)
        
        # Count log files
        log_dir = ws / "logs"
        if log_dir.exists():
            for f in log_dir.rglob("*"):
                if f.is_file():
                    stats["log_files"] += 1
                    stats["log_size_mb"] += f.stat().st_size / (1024 * 1024)
        
        # Count temp files
        temp_dir = ws / "temp"
        if temp_dir.exists():
            for f in temp_dir.rglob("*"):
                if f.is_file():
                    stats["temp_files"] += 1
                    stats["temp_size_mb"] += f.stat().st_size / (1024 * 1024)
    except Exception:
        pass
    
    return stats


# Helper: execution history IO
def _history_paths(ws_path_str: str):
    ws = Path(ws_path_str)
    logs_dir = ws / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    exec_file = logs_dir / "executions.json"
    return logs_dir, exec_file

def load_execution_history(ws_path_str: str) -> List[Dict[str, Any]]:
    try:
        _, exec_file = _history_paths(ws_path_str)
        if exec_file.exists():
            with open(exec_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []

def save_execution_history(ws_path_str: str, history: List[Dict[str, Any]]):
    try:
        _, exec_file = _history_paths(ws_path_str)
        with open(exec_file, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _append_log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.pipeline_logs.append(f"[{ts}] {msg}")

def _validate_step_args(workspace: str, step_name: str, args: Dict[str, Any]) -> Optional[str]:
    """
    Basic validation for common patterns.
    Returns None if OK; error string otherwise.
    """
    ws = Path(workspace)
    # Validate input_file exists when provided
    input_file = args.get("input_file")
    if input_file:
        f = ws / input_file
        if not f.exists():
            return f"Input file not found: {input_file}"
    # Specific checks
    if step_name == "normalize_csv":
        if not input_file:
            return "An input_file is required for normalization"
        if not args.get("output_file"):
            return "An output_file is required for normalization"
    return None


def render_sidebar():
    """Render sidebar with quick guide and system info"""
    with st.sidebar:
        st.markdown("### 📖 Quick Guide")
        st.markdown("""
        1. **Upload** your Excel/CSV file
        2. **Select** pipeline steps
        3. **Configure** arguments
        4. **Execute** the pipeline
        5. **Monitor** progress in real-time
        """)
        
        st.divider()
        
        st.markdown("### ℹ️ System Info")
        
        # Status
        st.markdown("**Status:** 🟢 Ready")
        
        # Files loaded
        st.markdown(f"**Files Loaded:** {st.session_state.files_loaded}")

        # Steps selected (enabled)
        try:
            st.session_state.steps_selected = sum(1 for k, v in st.session_state.step_enabled.items() if v)
        except Exception:
            pass
        st.markdown(f"**Steps Selected:** {st.session_state.steps_selected}")
        
        st.divider()
        
        # Version info
        st.caption("Asidelco Explorer v1.0")
        st.caption("Pipeline Management System")


# Update render_workspace_selector() to defer workspace creation
def render_workspace_selector():
    """Render workspace selector at the top"""
    wm = st.session_state.workspace_manager
    
    # Get all workspaces
    try:
        workspaces = wm.list_workspaces()
        workspace_list = []
        for ws in workspaces:
            if isinstance(ws, dict):
                workspace_list.append(ws)
            else:
                workspace_list.append({"workspace_path": str(ws), "workspace_name": Path(ws).name})
    except Exception:
        workspace_list = []
    
    # Workspace selector in columns
    col1, col2 = st.columns([2, 4])
    
    with col1:
        st.markdown("**📁 Workspace:**")
    
    with col2:
        # Prepare workspace options with "Create New" as default
        workspace_options = ["< Create New Workspace >"]
        workspace_map = {}
        
        for ws in sorted(workspace_list, key=lambda x: x.get("workspace_name", ""), reverse=True):
            ws_name = ws.get("workspace_name", "Unknown")
            ws_path_str = ws.get("workspace_path", "")
            workspace_options.append(ws_name)
            workspace_map[ws_name] = ws_path_str
        
        # Determine current selection index
        current_index = 0  # Default to "Create New"
        if st.session_state.current_workspace and not st.session_state.workspace_needs_creation:
            current_name = Path(st.session_state.current_workspace).name
            if current_name in workspace_options:
                current_index = workspace_options.index(current_name)
        
        # Workspace selector
        selected_ws_name = st.selectbox(
            "Select workspace",
            options=workspace_options,
            index=current_index,
            label_visibility="collapsed",
            key="workspace_selector"
        )
        
        # Handle workspace selection
        if selected_ws_name == "< Create New Workspace >":
            # Mark that new workspace will be created on pipeline start
            st.session_state.workspace_needs_creation = True
            st.session_state.current_workspace = None
            st.session_state.execution_history = []
            st.session_state.pipeline_logs = []
            st.info("ℹ️ New workspace will be created when pipeline starts")
        else:
            # Load existing workspace
            selected_path = workspace_map.get(selected_ws_name)
            if selected_path != st.session_state.current_workspace:
                try:
                    wm.load_workspace(selected_path)
                    st.session_state.current_workspace = selected_path
                    st.session_state.workspace_needs_creation = False
                    # Load execution history for this workspace
                    st.session_state.execution_history = load_execution_history(selected_path)
                    st.session_state.pipeline_logs = []
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to load workspace: {e}")

# Add function to create workspace when needed
def create_workspace_if_needed():
    """Create workspace if marked for creation"""
    if st.session_state.workspace_needs_creation:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        project_root = SRC_DIR.parent
        workspaces_root = project_root / "workspaces"
        workspaces_root.mkdir(parents=True, exist_ok=True)
        ws_path = workspaces_root / f"Ene-Mar25_{ts}_workdir"
        # Ensure standard subdirs
        (ws_path / "data" / "input").mkdir(parents=True, exist_ok=True)
        (ws_path / "data" / "output").mkdir(parents=True, exist_ok=True)
        (ws_path / "logs").mkdir(parents=True, exist_ok=True)
        (ws_path / "temp").mkdir(parents=True, exist_ok=True)

        try:
            wm = st.session_state.workspace_manager
            wm.load_workspace(str(ws_path))
            st.session_state.current_workspace = str(ws_path)
            st.session_state.workspace_needs_creation = False
            # Initialize empty history
            save_execution_history(str(ws_path), [])
            st.session_state.execution_history = []
            return str(ws_path)
        except Exception as e:
            st.error(f"Failed to create workspace: {e}")
            return None
    return st.session_state.current_workspace

def _execute_steps(stage_idx: int, stage_data: dict, step_indices: Optional[List[int]] = None) -> bool:
    """
    Internal executor used by execute_stage and retry handlers.
    """
    try:
        from pipeline.pipeline import Pipeline

        # Ensure workspace exists
        workspace = create_workspace_if_needed()
        if not workspace:
            st.error("Failed to create workspace")
            return False

        pipeline = Pipeline(workspace_dir=workspace)
        stage_title = stage_data.get("title", f"Stage {stage_idx}")
        steps = stage_data.get("steps", [])
        indices = step_indices if step_indices is not None else list(range(len(steps)))

        st.session_state.pipeline_running = True
        st.session_state.stop_requested = False
        stage_key = f"stage_{stage_idx}"
        st.session_state.stage_progress[stage_key] = {"status": "running"}
        _append_log(f"Stage '{stage_title}' started")

        # Prepare run record
        run_record = {
            "workspace": workspace,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "stage_index": stage_idx,
            "stage_title": stage_title,
            "steps": []
        }

        # Map step names to their string identifiers (matching registry keys)
        step_type_map = {
            "normalize_csv": "normalize_csv",
            "crawl_projects": "crawl_projects",
            "crawl_professionals": "crawl_professionals",
            "parse_html": "parse_html",
            "transform_data": "transform_data",
            "generate_embeddings": "generate_embeddings",
            "load_opensearch": "load_opensearch",
        }

        # Create status placeholders for real-time updates
        status_container = st.empty()
        progress_bar = st.empty()

        for idx in indices:
            step = steps[idx]
            step_enabled_key = f"enabled_{stage_idx}_{idx}"

            # Skip if disabled
            if not st.session_state.step_enabled.get(step_enabled_key, True):
                step_name = step.get("name", f"step_{idx}")
                step_title_val = step.get("title", step_name)
                step_key = f"{step_name}_{step_title_val}"
                st.session_state.step_status[step_key] = {
                    "status": "skipped",
                    "progress": 0,
                    "message": "Step skipped by user"
                }
                run_record["steps"].append({
                    "step_index": idx,
                    "step_name": step_name,
                    "title": step_title_val,
                    "state": "skipped",
                    "start_time": None,
                    "end_time": None,
                    "duration": None
                })
                continue

            # Stop requested?
            if st.session_state.stop_requested:
                _append_log(f"Stop requested. Aborting stage '{stage_title}' after current step.")
                break

            step_name = step.get("name", f"step_{idx}")
            step_title_val = step.get("title", step_name)
            step_key = f"{step_name}_{step_title_val}"
            step_args_key = f"args_{stage_idx}_{idx}"
            custom_args = st.session_state.step_args.get(step_args_key, step.get("args", {}))

            # Update status with real-time UI feedback
            st.session_state.step_status[step_key] = {
                "status": "running",
                "progress": 0,
                "message": f"Executing {step_title_val}..."
            }

            # Show current step status
            status_container.info(f"🔄 Running: {step_title_val}")
            progress_bar.progress(0)

            # Validation
            err = _validate_step_args(workspace, step_name, custom_args)
            if err:
                st.session_state.step_status[step_key] = {
                    "status": "failed",
                    "progress": 0,
                    "message": err
                }
                status_container.error(f"❌ {step_title_val} failed: {err}")
                _append_log(f"Step '{step_title_val}' failed: {err}")
                st.session_state.stage_progress[stage_key] = {"status": "failed"}
                st.session_state.pipeline_running = False
                # Append to record
                run_record["steps"].append({
                    "step_index": idx,
                    "step_name": step_name,
                    "title": step_title_val,
                    "state": "failed",
                    "start_time": None,
                    "end_time": None,
                    "duration": None
                })
                st.session_state.execution_history.append(run_record)
                save_execution_history(workspace, st.session_state.execution_history)
                return False

            start_t = time.time()
            start_iso = datetime.now().isoformat(timespec="seconds")
            try:
                step_type = step_type_map.get(step_name)
                if not step_type:
                    raise ValueError(f"Unknown step type: {step_name}")

                _append_log(f"Step '{step_title_val}' started")

                # Show progress
                progress_bar.progress(25)

                # Execute step
                pipeline.add_step(step_type, **custom_args)
                progress_bar.progress(50)

                pipeline.run()
                progress_bar.progress(100)

                st.session_state.step_status[step_key] = {
                    "status": "completed",
                    "progress": 100,
                    "message": f"✅ Completed successfully"
                }
                status_container.success(f"✅ {step_title_val} completed")
                _append_log(f"Step '{step_title_val}' completed")

                end_t = time.time()
                end_iso = datetime.now().isoformat(timespec="seconds")
                run_record["steps"].append({
                    "step_index": idx,
                    "step_name": step_name,
                    "title": step_title_val,
                    "state": "completed",
                    "start_time": start_iso,
                    "end_time": end_iso,
                    "duration": round(end_t - start_t, 2)
                })

                # Small delay to show completion
                time.sleep(0.5)

            except Exception as e:
                st.session_state.step_status[step_key] = {
                    "status": "failed",
                    "progress": 0,
                    "message": f"Error: {str(e)}"
                }
                status_container.error(f"❌ {step_title_val} failed: {str(e)}")
                _append_log(f"Step '{step_title_val}' failed: {str(e)}")

                end_t = time.time()
                end_iso = datetime.now().isoformat(timespec="seconds")
                run_record["steps"].append({
                    "step_index": idx,
                    "step_name": step_name,
                    "title": step_title_val,
                    "state": "failed",
                    "start_time": start_iso,
                    "end_time": end_iso,
                    "duration": round(end_t - start_t, 2)
                })

                st.session_state.stage_progress[stage_key] = {"status": "failed"}
                st.session_state.pipeline_running = False
                # Persist run record
                st.session_state.execution_history.append(run_record)
                save_execution_history(workspace, st.session_state.execution_history)
                return False

        # Clear status displays
        status_container.empty()
        progress_bar.empty()

        # Done or stopped
        if st.session_state.stop_requested:
            st.session_state.stage_progress[stage_key] = {"status": "pending"}
            st.session_state.pipeline_running = False
            _append_log(f"Stage '{stage_title}' stopped by user")
            st.warning(f"⏹ Stage '{stage_title}' stopped")
        else:
            st.session_state.stage_progress[stage_key] = {"status": "completed"}
            st.session_state.pipeline_running = False
            _append_log(f"Stage '{stage_title}' completed")
            st.success(f"🎉 Stage '{stage_title}' completed successfully!")

        # Persist run record
        st.session_state.execution_history.append(run_record)
        save_execution_history(workspace, st.session_state.execution_history)
        return not st.session_state.stop_requested

    except Exception as e:
        st.error(f"Stage execution failed: {str(e)}")
        st.session_state.pipeline_running = False
        stage_key = f"stage_{stage_idx}"
        st.session_state.stage_progress[stage_key] = {"status": "failed"}
        _append_log(f"Stage failed: {str(e)}")
        # Try to persist whatever we have
        if st.session_state.current_workspace:
            save_execution_history(st.session_state.current_workspace, st.session_state.execution_history)
        return False

def execute_stage(stage_idx: int, stage_data: dict):
    """Execute a pipeline stage with selected steps"""
    return _execute_steps(stage_idx, stage_data, step_indices=None)

def execute_specific_steps(stage_idx: int, stage_data: dict, indices: List[int]):
    """Execute only specific steps within a stage"""
    return _execute_steps(stage_idx, stage_data, step_indices=indices)

# Update render_workspace_section() to handle no workspace
def render_workspace_section():
    """Render workspace management section"""
    st.markdown('<div class="section-header">📁 Workspace Details</div>', unsafe_allow_html=True)
    
    current_ws = st.session_state.current_workspace
    
    if not current_ws or st.session_state.workspace_needs_creation:
        st.info("ℹ️ Workspace will be created when you start the pipeline")
        return
    
    # Workspace details
    with st.expander("📊 Information", expanded=False):
        ws_path = Path(current_ws)
        stats = get_workspace_stats(current_ws)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Created:**")
            try:
                created = datetime.fromtimestamp(ws_path.stat().st_ctime)
                st.text(created.strftime("%Y-%m-%d %H:%M:%S"))
            except Exception:
                st.text("Unknown")
            
            st.markdown("**Source File:**")
            input_dir = ws_path / "data" / "input"
            if input_dir.exists():
                files = list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.csv"))
                if files:
                    st.text(files[0].name)
                else:
                    st.text("No file")
            else:
                st.text("No file")
        
        with col2:
            st.markdown("**data/input:**")
            st.text(f"{stats['input_files']} files ({stats['input_size_mb']:.2f} MB)")
            
            st.markdown("**data/output:**")
            st.text(f"{stats['output_files']} files ({stats['output_size_mb']:.2f} MB)")
            
            st.markdown("**logs:**")
            st.text(f"{stats['log_files']} files ({stats['log_size_mb']:.2f} MB)")
        
        st.caption(f"📂 {current_ws}")

def render_workspace_summary():
    """Render workspace summary section"""
    st.markdown('<div class="section-header">📊 Workspace Summary</div>', unsafe_allow_html=True)

    current_ws = st.session_state.current_workspace
    if not current_ws:
        st.info("No workspace selected")
        return

    ws_path = Path(current_ws)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("**Workspace Location:**")
        st.code(current_ws)

        # Show metadata if exists
        metadata_file = ws_path / "workspace_metadata.json"
        if metadata_file.exists():
            try:
                with open(metadata_file) as f:
                    metadata = json.load(f)
                st.json(metadata, expanded=False)
            except Exception:
                pass

        # Execution history table
        history = st.session_state.execution_history or load_execution_history(current_ws)
        if history:
            st.markdown("**Recent Executions:**")
            # Show last 5
            rows = []
            for run in history[-5:]:
                steps = run.get("steps", [])
                completed = sum(1 for s in steps if s.get("state") == "completed")
                failed = sum(1 for s in steps if s.get("state") == "failed")
                rows.append(f"- {run.get('started_at')} • {run.get('stage_title')} • {completed} ok / {failed} failed")
            st.markdown("\n".join(rows))

    with col2:
        st.markdown("**Output Files:**")
        output_dir = ws_path / "data" / "output"
        if output_dir.exists():
            for f in sorted(output_dir.glob("*")):
                if f.is_file():
                    st.markdown(f"📄 {f.name}")
        else:
            st.caption("No outputs yet")


def render_step_progress(step_title: str, step_name: str, step_data: dict, stage_idx: int, step_idx: int, status: str = "pending"):
    """Render progress for a single step with controls"""
    status_icons = {
        "pending": "⏳",
        "running": "🔄",
        "completed": "✅",
        "failed": "❌",
        "skipped": "⏭️"
    }
    
    status_colors = {
        "pending": "#6c757d",
        "running": "#0d6efd",
        "completed": "#28a745",
        "failed": "#dc3545",
        "skipped": "#ffc107"
    }
    
    icon = status_icons.get(status, "⏳")
    color = status_colors.get(status, "#6c757d")
    
    # Get progress info if exists
    step_key = f"{step_name}_{step_title}"
    progress_info = st.session_state.step_status.get(step_key, {})
    progress = progress_info.get("progress", 0)
    message = progress_info.get("message", "")
    
    # Step container
    with st.container():
        # Header with checkbox
        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            # Enable/disable checkbox
            step_enabled_key = f"enabled_{stage_idx}_{step_idx}"
            is_enabled = st.session_state.step_enabled.get(step_enabled_key, True)

            new_enabled = st.checkbox(
                f"{icon} **{step_title}**",
                value=is_enabled,
                key=step_enabled_key,
                disabled=(status == "running")
            )

            st.session_state.step_enabled[step_enabled_key] = new_enabled

            if message:
                st.caption(f"💬 {message}")

        with col2:
            st.markdown(f"""
            <div style="
                padding: 0.25rem 0.75rem;
                border-radius: 1rem;
                background-color: {color};
                color: white;
                font-size: 0.875rem;
                font-weight: 500;
                text-align: center;
            ">
                {status.upper()}
            </div>
            """, unsafe_allow_html=True)

        with col3:
            # Retry button for failed/skipped steps
            if status in ("failed", "skipped") and not st.session_state.pipeline_running:
                if st.button("↻ Retry", key=f"retry_{stage_idx}_{step_idx}", use_container_width=True):
                    execute_specific_steps(stage_idx, {"title": f"Retry {step_title}", "steps": [step_data]}, [0])
                    st.rerun()
        
        # Show progress bar if running
        if status == "running" and progress > 0:
            st.progress(progress / 100)
        
        # Step arguments editor
        if new_enabled:
            # Special handling for "Select Excel input file" step in Ingest stage
            if step_name == "normalize_csv" and stage_idx == 0:
                render_excel_upload_step(step_data, stage_idx, step_idx)
            else:
                render_standard_step_config(step_data, stage_idx, step_idx)
        
        st.markdown("---")


# Update render_excel_upload_step() to handle no workspace
def render_excel_upload_step(step_data: dict, stage_idx: int, step_idx: int):
    """Render Excel file upload/selection for Ingest stage"""
    with st.expander(f"⚙️ Configure", expanded=True):
        # If no workspace, show info message
        if st.session_state.workspace_needs_creation or not st.session_state.current_workspace:
            st.info("ℹ️ Workspace will be created when you start the pipeline. You can upload a file after workspace is created.")
            
            step_args_key = f"args_{stage_idx}_{step_idx}"
            args = step_data.get("args", {})
            if step_args_key not in st.session_state.step_args:
                st.session_state.step_args[step_args_key] = args.copy()
            return
        
        current_ws = st.session_state.current_workspace
        ws_path = Path(current_ws)
        input_dir = ws_path / "data" / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        
        # Check for existing files
        existing_files = list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.csv"))
        
        # File selection mode
        file_mode = st.radio(
            "Choose input method:",
            ["Upload new file", "Select existing file"],
            horizontal=True,
            key=f"file_mode_{stage_idx}_{step_idx}"
        )
        
        step_args_key = f"args_{stage_idx}_{step_idx}"
        args = step_data.get("args", {})
        
        if step_args_key not in st.session_state.step_args:
            st.session_state.step_args[step_args_key] = args.copy()
        
        if file_mode == "Upload new file":
            uploaded_file = st.file_uploader(
                "Choose an Excel file",
                type=["xlsx", "csv"],
                key=f"file_upload_{stage_idx}_{step_idx}"
            )
            
            if uploaded_file:
                file_path = input_dir / uploaded_file.name
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                st.success(f"✅ Uploaded: {uploaded_file.name}")
                st.session_state.files_loaded = 1
                
                st.session_state.step_args[step_args_key] = {
                    "input_file": f"data/input/{uploaded_file.name}",
                    "output_file": args.get("output_file", "data/output/normalized.csv")
                }
                
                st.info(f"📊 Size: {uploaded_file.size / (1024*1024):.2f} MB")
        else:
            if existing_files:
                file_options = [f.name for f in existing_files]
                selected_file = st.selectbox(
                    "Select existing file:",
                    options=file_options,
                    key=f"file_select_{stage_idx}_{step_idx}"
                )
                
                if selected_file:
                    st.session_state.step_args[step_args_key] = {
                        "input_file": f"data/input/{selected_file}",
                        "output_file": args.get("output_file", "data/output/normalized.csv")
                    }
                    
                    file_path = input_dir / selected_file
                    file_size = file_path.stat().st_size / (1024*1024)
                    st.info(f"📊 Selected: {selected_file} ({file_size:.2f} MB)")
            else:
                st.warning("⚠️ No files in workspace. Upload a file.")
        
        st.markdown("**Output:**")
        output_file = st.text_input(
            "Filename:",
            value=st.session_state.step_args[step_args_key].get("output_file", "data/output/normalized.csv"),
            key=f"output_file_{stage_idx}_{step_idx}",
            label_visibility="collapsed"
        )
        
        if step_args_key in st.session_state.step_args:
            st.session_state.step_args[step_args_key]["output_file"] = output_file

def _get_workspace_files_and_dirs(workspace_path: Path, pattern: str = "*"):
    """Get files and directories from workspace matching pattern"""
    if not workspace_path.exists():
        return [], []

    files = []
    dirs = []

    # Search in data/input and data/output
    search_dirs = [
        workspace_path / "data" / "input",
        workspace_path / "data" / "output"
    ]

    for search_dir in search_dirs:
        if search_dir.exists():
            # Get files
            for item in search_dir.rglob(pattern):
                if item.is_file():
                    # Get relative path from workspace
                    try:
                        rel_path = item.relative_to(workspace_path)
                        files.append(str(rel_path))
                    except ValueError:
                        pass

            # Get directories (only immediate subdirectories and their children)
            for item in search_dir.rglob("*"):
                if item.is_dir() and item != search_dir:
                    try:
                        rel_path = item.relative_to(workspace_path)
                        dirs.append(str(rel_path))
                    except ValueError:
                        pass

    return sorted(files), sorted(dirs)


def _is_path_argument(arg_name: str, arg_value: str) -> tuple[bool, str]:
    """Check if argument is a file/directory path and return type"""
    arg_lower = arg_name.lower()

    # Check for input file
    if 'input_file' in arg_lower or arg_lower == 'input_file':
        return True, 'input_file'

    # Check for output file
    if 'output_file' in arg_lower or arg_lower == 'output_file':
        return True, 'output_file'

    # Check for input directory
    if 'input_dir' in arg_lower or arg_lower == 'input_dir' or arg_lower == 'input_directory':
        return True, 'input_dir'

    # Check for output directory
    if 'output_dir' in arg_lower or arg_lower == 'output_dir' or arg_lower == 'output_directory':
        return True, 'output_dir'

    # Check for generic path/file/dir keywords
    if any(keyword in arg_lower for keyword in ['file', 'path', 'dir', 'directory']):
        # Try to guess based on value
        if isinstance(arg_value, str):
            if 'output' in arg_lower:
                if any(ext in arg_value for ext in ['.csv', '.json', '.xlsx', '.txt']):
                    return True, 'output_file'
                else:
                    return True, 'output_dir'
            elif 'input' in arg_lower:
                if any(ext in arg_value for ext in ['.csv', '.json', '.xlsx', '.txt', '.html']):
                    return True, 'input_file'
                else:
                    return True, 'input_dir'

    return False, ''


# Update render_standard_step_config() with workspace file browser
def render_standard_step_config(step_data: dict, stage_idx: int, step_idx: int):
    """Render standard step configuration with workspace file browser"""
    with st.expander(f"⚙️ Configure", expanded=False):
        args = step_data.get("args", {})
        step_args_key = f"args_{stage_idx}_{step_idx}"

        if step_args_key not in st.session_state.step_args:
            st.session_state.step_args[step_args_key] = args.copy()

        edited_args = {}

        if not args:
            st.info("No parameters")
            return

        # Get workspace path
        workspace_path = None
        if st.session_state.current_workspace:
            workspace_path = Path(st.session_state.current_workspace)

        for arg_name, arg_value in args.items():
            arg_input_key = f"arg_{stage_idx}_{step_idx}_{arg_name}"

            if isinstance(arg_value, dict):
                st.markdown(f"**{arg_name}:**")
                st.json(arg_value, expanded=False)
                edited_args[arg_name] = arg_value
            elif isinstance(arg_value, list):
                st.markdown(f"**{arg_name}:**")
                st.json(arg_value, expanded=False)
                edited_args[arg_name] = arg_value
            elif isinstance(arg_value, bool):
                edited_args[arg_name] = st.checkbox(
                    arg_name,
                    value=arg_value,
                    key=arg_input_key
                )
            elif isinstance(arg_value, int):
                # Special handling for specific parameters
                if arg_name.lower() in ['max_retries', 'max_members']:
                    edited_args[arg_name] = st.number_input(
                        arg_name,
                        value=arg_value,
                        min_value=1,
                        max_value=100,
                        step=1,
                        key=arg_input_key,
                        help=f"Number of retry attempts (1-100)" if 'retries' in arg_name.lower() else None
                    )
                elif arg_name.lower() == 'timeout':
                    edited_args[arg_name] = st.number_input(
                        arg_name,
                        value=arg_value,
                        min_value=5,
                        max_value=300,
                        step=5,
                        key=arg_input_key,
                        help="Request timeout in seconds (5-300)"
                    )
                elif arg_name.lower() == 'port':
                    edited_args[arg_name] = st.number_input(
                        arg_name,
                        value=arg_value,
                        min_value=1,
                        max_value=65535,
                        step=1,
                        key=arg_input_key,
                        help="Port number (1-65535)"
                    )
                else:
                    edited_args[arg_name] = st.number_input(
                        arg_name,
                        value=arg_value,
                        key=arg_input_key
                    )
            elif isinstance(arg_value, float):
                # Special handling for rate_limit
                if arg_name.lower() == 'rate_limit':
                    edited_args[arg_name] = st.number_input(
                        arg_name,
                        value=arg_value,
                        min_value=0.1,
                        max_value=10.0,
                        step=0.1,
                        format="%.1f",
                        key=arg_input_key,
                        help="Seconds to wait between requests (0.1-10.0)"
                    )
                else:
                    edited_args[arg_name] = st.number_input(
                        arg_name,
                        value=arg_value,
                        format="%.2f",
                        key=arg_input_key
                    )
            else:
                # Check if this is a file/directory path argument
                is_path, path_type = _is_path_argument(arg_name, str(arg_value))

                if is_path and workspace_path and workspace_path.exists():
                    st.markdown(f"**{arg_name}:**")

                    # Selection mode
                    selection_mode = st.radio(
                        "Selection mode:",
                        ["Browse workspace", "Manual entry"],
                        horizontal=True,
                        key=f"{arg_input_key}_mode",
                        label_visibility="collapsed"
                    )

                    if selection_mode == "Browse workspace":
                        if path_type == 'input_file':
                            # Show file browser for input files
                            files, _ = _get_workspace_files_and_dirs(workspace_path, "*")
                            if files:
                                # Filter to common input file types
                                input_files = [f for f in files if any(f.endswith(ext) for ext in ['.csv', '.xlsx', '.json', '.html', '.txt'])]
                                if input_files:
                                    selected = st.selectbox(
                                        f"Select {arg_name}:",
                                        options=[""] + input_files,
                                        index=input_files.index(str(arg_value)) + 1 if str(arg_value) in input_files else 0,
                                        key=f"{arg_input_key}_select",
                                        label_visibility="collapsed"
                                    )
                                    edited_args[arg_name] = selected if selected else str(arg_value)
                                else:
                                    st.warning("⚠️ No input files found in workspace")
                                    edited_args[arg_name] = st.text_input(
                                        "Path:",
                                        value=str(arg_value),
                                        key=f"{arg_input_key}_fallback",
                                        label_visibility="collapsed"
                                    )
                            else:
                                st.warning("⚠️ No files found in workspace")
                                edited_args[arg_name] = st.text_input(
                                    "Path:",
                                    value=str(arg_value),
                                    key=f"{arg_input_key}_fallback",
                                    label_visibility="collapsed"
                                )

                        elif path_type == 'input_dir':
                            # Show directory browser for input directories
                            _, dirs = _get_workspace_files_and_dirs(workspace_path)
                            if dirs:
                                # Add common output directories
                                all_dirs = dirs + ["data/output/projects/html", "data/output/professionals/html"]
                                all_dirs = sorted(list(set(all_dirs)))
                                selected = st.selectbox(
                                    f"Select {arg_name}:",
                                    options=[""] + all_dirs,
                                    index=all_dirs.index(str(arg_value)) + 1 if str(arg_value) in all_dirs else 0,
                                    key=f"{arg_input_key}_select",
                                    label_visibility="collapsed"
                                )
                                edited_args[arg_name] = selected if selected else str(arg_value)
                            else:
                                st.info("ℹ️ Directory will be created if it doesn't exist")
                                edited_args[arg_name] = st.text_input(
                                    "Path:",
                                    value=str(arg_value),
                                    key=f"{arg_input_key}_fallback",
                                    label_visibility="collapsed"
                                )

                        elif path_type in ['output_file', 'output_dir']:
                            # For output paths, just show text input with helper text
                            st.caption("💡 Output path (will be created if needed)")
                            edited_args[arg_name] = st.text_input(
                                "Path:",
                                value=str(arg_value),
                                key=f"{arg_input_key}_output",
                                label_visibility="collapsed"
                            )

                        else:
                            # Generic path handling
                            files, dirs = _get_workspace_files_and_dirs(workspace_path)
                            all_paths = files + dirs
                            if all_paths:
                                selected = st.selectbox(
                                    f"Select {arg_name}:",
                                    options=[""] + all_paths,
                                    index=all_paths.index(str(arg_value)) + 1 if str(arg_value) in all_paths else 0,
                                    key=f"{arg_input_key}_select",
                                    label_visibility="collapsed"
                                )
                                edited_args[arg_name] = selected if selected else str(arg_value)
                            else:
                                edited_args[arg_name] = st.text_input(
                                    "Path:",
                                    value=str(arg_value),
                                    key=f"{arg_input_key}_fallback",
                                    label_visibility="collapsed"
                                )
                    else:
                        # Manual entry mode
                        edited_args[arg_name] = st.text_input(
                            "Path:",
                            value=str(arg_value),
                            key=f"{arg_input_key}_manual",
                            label_visibility="collapsed"
                        )
                else:
                    # Regular text input for non-path arguments
                    edited_args[arg_name] = st.text_input(
                        arg_name,
                        value=str(arg_value),
                        key=arg_input_key
                    )

        st.session_state.step_args[step_args_key] = edited_args

# Update render_pipeline_config() stage header
def render_pipeline_config():
    """Render pipeline configuration with horizontal tabs for stages"""
    st.markdown('<div class="section-header">⚙️ Pipeline Configuration</div>', unsafe_allow_html=True)
    
    cfg, cfg_path, err = load_base_pipeline_config()
    
    if err:
        st.error(f"❌ {err}")
        return
    
    if not cfg:
        st.warning("⚠️ No configuration found")
        return
    
    pipeline_info = cfg.get("pipeline", cfg)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Pipeline", pipeline_info.get("name", "Unknown")[:15])
    with col2:
        st.metric("Version", cfg.get("version", "1.0"))
    with col3:
        stages = pipeline_info.get("stages", [])
        total_steps = sum(len(stage.get("steps", [])) for stage in stages)
        st.metric("Total Steps", total_steps)
    with col4:
        enabled_steps = sum(1 for key, val in st.session_state.step_enabled.items() if val)
        st.metric("Enabled", f"{enabled_steps}/{total_steps}")
    
    # Show relative path if it's in the project
    if cfg_path:
        try:
            relative_path = cfg_path.relative_to(Path.cwd())
            st.caption(f"📄 Config: {relative_path}")
        except ValueError:
            st.caption(f"📄 Config: {cfg_path}")
    
    if "description" in pipeline_info:
        with st.expander("ℹ️ Description", expanded=False):
            st.info(pipeline_info["description"])
    
    st.divider()
    
    if not stages:
        st.warning("No stages defined")
        return
    
    tab_labels = []
    stage_status_icons = {
        "pending": "⏳",
        "running": "🔄",
        "completed": "✅",
        "failed": "❌"
    }
    
    for i, stage in enumerate(stages):
        stage_title = stage.get("title", f"Stage {i+1}")
        stage_key = f"stage_{i}"
        stage_info = st.session_state.stage_progress.get(stage_key, {"status": "pending"})
        status_icon = stage_status_icons.get(stage_info.get("status", "pending"), "⏳")
        tab_labels.append(f"{status_icon} {stage_title}")
    
    selected_tab = st.tabs(tab_labels)
    
    # Render each stage
    for i, (tab, stage) in enumerate(zip(selected_tab, stages)):
        with tab:
            stage_title = stage.get("title", f"Stage {i+1}")
            steps = stage.get("steps", [])
            
            # Stage header
            col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
            with col1:
                st.markdown(f"### {stage_title}")
            with col2:
                if st.button(f"✅ All", key=f"enable_all_{i}", use_container_width=True):
                    for j in range(len(steps)):
                        st.session_state.step_enabled[f"enabled_{i}_{j}"] = True
                    st.rerun()
            with col3:
                if st.button(f"❌ None", key=f"disable_all_{i}", use_container_width=True):
                    for j in range(len(steps)):
                        st.session_state.step_enabled[f"enabled_{i}_{j}"] = False
                    st.rerun()
            with col4:
                stage_key = f"stage_{i}"
                stage_info_state = st.session_state.stage_progress.get(stage_key, {"status": "pending"})

                # Disable run button if already running
                is_running = stage_info_state.get("status") == "running" or st.session_state.pipeline_running

                if not is_running:
                    if st.button(f"▶️ Run", key=f"run_stage_{i}", use_container_width=True, type="primary"):
                        # Execute stage WITHOUT spinner to see real-time updates
                        _append_log(f"User clicked Run for stage {i}: {stage_title}")
                        success = execute_stage(i, stage)
                        if success:
                            st.balloons()
                        # Force rerun to show final state
                        st.rerun()
                else:
                    if st.button(f"⏹ Stop", key=f"stop_stage_{i}", use_container_width=True):
                        st.session_state.stop_requested = True
                        _append_log("User requested stop")
                        st.rerun()
            
            st.divider()
            
            # Render steps
            if not steps:
                st.info("No steps")
            else:
                for j, step in enumerate(steps):
                    step_name = step.get("name", f"step_{j}")
                    step_title_text = step.get("title", step_name)
                    
                    step_key = f"{step_name}_{step_title_text}"
                    step_info = st.session_state.step_status.get(step_key, {"status": "pending"})
                    
                    render_step_progress(step_title_text, step_name, step, i, j, step_info.get("status", "pending"))
                
                # Progress summary + retry failed
                st.divider()
                total = len(steps)
                completed = sum(1 for step in steps
                               if st.session_state.step_status.get(f"{step.get('name')}_{step.get('title')}", {}).get("status") == "completed")
                enabled = sum(1 for j in range(len(steps)) if st.session_state.step_enabled.get(f"enabled_{i}_{j}", True))
                failed_indices = [idx for idx, step in enumerate(steps)
                                  if st.session_state.step_status.get(f"{step.get('name')}_{step.get('title')}", {}).get("status") == "failed"]

                col1, col2, col3 = st.columns([3, 1, 1])
                with col1:
                    st.progress(completed / total if total else 0)
                    st.caption(f"Progress: {completed}/{total} completed • {enabled} enabled")
                with col2:
                    if failed_indices and not st.session_state.pipeline_running:
                        if st.button("↻ Retry Failed", key=f"retry_failed_{i}", use_container_width=True):
                            _append_log(f"User clicked Retry Failed for stage {i}")
                            execute_specific_steps(i, stage, failed_indices)
                            st.rerun()
                with col3:
                    # already used above for disable all; keep empty here to keep layout compact
                    st.empty()

def render_pipeline_monitor():
    """Render real-time pipeline monitoring section with simple progress bars"""
    st.markdown('<div class="section-header">📊 Pipeline Monitor</div>', unsafe_allow_html=True)

    # Determine source for summary: running -> session, else last run
    if st.session_state.pipeline_running:
        running = True
        steps_status = list(st.session_state.step_status.values())
    else:
        running = False
        steps_status = []
        # Use last execution of current workspace
        if st.session_state.execution_history:
            last_run = st.session_state.execution_history[-1]
            for s in last_run.get("steps", []):
                steps_status.append({"status": s.get("state", "pending")})

    # Summary counts
    running_steps = sum(1 for s in steps_status if s.get("status") == "running")
    completed_steps = sum(1 for s in steps_status if s.get("status") == "completed")
    failed_steps = sum(1 for s in steps_status if s.get("status") == "failed")
    skipped_steps = sum(1 for s in steps_status if s.get("status") == "skipped")
    total_steps = len(steps_status) if steps_status else 0
    percentage = (completed_steps / total_steps) if total_steps else 0.0

    # Status summary
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Status", "🔄 Running" if st.session_state.pipeline_running else "⏸️ Idle")
    with col2:
        st.metric("Running", running_steps)
    with col3:
        st.metric("Completed", completed_steps)
    with col4:
        st.metric("Failed", failed_steps)
    with col5:
        st.metric("Skipped", skipped_steps)

    st.divider()

    # Overall progress bar
    st.markdown("**Overall Progress:**")
    if total_steps > 0:
        st.progress(percentage, text=f"{completed_steps}/{total_steps} steps completed ({percentage*100:.1f}%)")
    else:
        st.progress(0, text="No steps executed yet")

    st.divider()

    # Live log viewer
    with st.expander("📝 Live Logs", expanded=True):
        log_container = st.container()
        with log_container:
            if hasattr(st.session_state, 'pipeline_logs') and st.session_state.pipeline_logs:
                for log in st.session_state.pipeline_logs[-20:]:  # Show last 20 logs
                    st.text(log)
            else:
                st.caption("No logs available yet...")


def main():
    """Main application"""
    initialize_session_state()
    
    # Main content
    st.markdown('<div class="main-header">⚙️ Asidelco Explorer</div>', unsafe_allow_html=True)
    st.caption("Pipeline Management System - Workspace & Configuration Viewer")
    
    st.divider()
    
    # Workspace selector at the top
    render_workspace_selector()
    
    st.divider()
    
    # Render sidebar
    render_sidebar()
    
    # Workspace details (collapsed by default)
    render_workspace_section()
    
    st.divider()
    
    # Pipeline configuration with tabs
    render_pipeline_config()
    
    st.divider()
    
    # Real-time monitoring
    render_pipeline_monitor()
    
    st.divider()
    
    # Workspace summary
    render_workspace_summary()


if __name__ == "__main__":
    main()