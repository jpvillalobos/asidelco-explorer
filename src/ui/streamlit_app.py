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
        
        # Steps selected
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
            st.info("ℹ️ New workspace will be created when pipeline starts")
        else:
            # Load existing workspace
            selected_path = workspace_map.get(selected_ws_name)
            if selected_path != st.session_state.current_workspace:
                try:
                    wm.load_workspace(selected_path)
                    st.session_state.current_workspace = selected_path
                    st.session_state.workspace_needs_creation = False
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
        ws_path.mkdir(parents=True, exist_ok=True)
        
        try:
            wm = st.session_state.workspace_manager
            wm.load_workspace(str(ws_path))
            st.session_state.current_workspace = str(ws_path)
            st.session_state.workspace_needs_creation = False
            return str(ws_path)
        except Exception as e:
            st.error(f"Failed to create workspace: {e}")
            return None
    return st.session_state.current_workspace

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
    
    with col2:
        st.markdown("**Output Files:**")
        output_dir = ws_path / "data" / "output"
        if output_dir.exists():
            for f in sorted(output_dir.glob("*")):
                if f.is_file():
                    st.markdown(f"📄 {f.name}")


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
        col1, col2 = st.columns([4, 1])
        
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
                    "output_file": args.get("output_file", "normalized.csv")
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
                        "output_file": args.get("output_file", "normalized.csv")
                    }
                    
                    file_path = input_dir / selected_file
                    file_size = file_path.stat().st_size / (1024*1024)
                    st.info(f"📊 Selected: {selected_file} ({file_size:.2f} MB)")
            else:
                st.warning("⚠️ No files in workspace. Upload a file.")
        
        st.markdown("**Output:**")
        output_file = st.text_input(
            "Filename:",
            value=st.session_state.step_args[step_args_key].get("output_file", "normalized.csv"),
            key=f"output_file_{stage_idx}_{step_idx}",
            label_visibility="collapsed"
        )
        
        if step_args_key in st.session_state.step_args:
            st.session_state.step_args[step_args_key]["output_file"] = output_file

# Update render_standard_step_config() with smaller fonts
def render_standard_step_config(step_data: dict, stage_idx: int, step_idx: int):
    """Render standard step configuration without JSON preview"""
    with st.expander(f"⚙️ Configure", expanded=False):
        args = step_data.get("args", {})
        step_args_key = f"args_{stage_idx}_{step_idx}"
        
        if step_args_key not in st.session_state.step_args:
            st.session_state.step_args[step_args_key] = args.copy()
        
        edited_args = {}
        
        if not args:
            st.info("No parameters")
            return
        
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
                edited_args[arg_name] = st.number_input(
                    arg_name,
                    value=arg_value,
                    key=arg_input_key
                )
            elif isinstance(arg_value, float):
                edited_args[arg_name] = st.number_input(
                    arg_name,
                    value=arg_value,
                    format="%.2f",
                    key=arg_input_key
                )
            else:
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
    
    st.caption(f"📄 {cfg_path}")
    
    if "description" in pipeline_info:
        with st.expander("ℹ️ Description", expanded=False):
            st.info(pipeline_info["description"])
    
    st.divider()
    
    # ...existing code for tabs...
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
            
            # Stage header - smaller
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.markdown(f"### {stage_title}")
            with col2:
                if st.button(f"✅ All", key=f"enable_all_{i}", use_container_width=True):
                    for j in range(len(steps)):
                        st.session_state.step_enabled[f"enabled_{i}_{j}"] = True
                    st.rerun()
            with col3:
                stage_key = f"stage_{i}"
                stage_info = st.session_state.stage_progress.get(stage_key, {"status": "pending"})
                
                if stage_info.get("status") != "running":
                    if st.button(f"▶️ Run", key=f"run_stage_{i}", use_container_width=True, type="primary"):
                        # Create workspace if needed
                        workspace = create_workspace_if_needed()
                        if workspace:
                            st.session_state.stage_progress[stage_key] = {"status": "running"}
                            st.session_state.pipeline_running = True
                            st.success(f"Starting {stage_title}...")
                            st.rerun()
                else:
                    if st.button(f"⏸️ Stop", key=f"pause_stage_{i}", use_container_width=True):
                        st.session_state.stage_progress[stage_key] = {"status": "pending"}
                        st.session_state.pipeline_running = False
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
                
                # Progress summary
                st.divider()
                completed = sum(1 for step in steps 
                               if st.session_state.step_status.get(f"{step.get('name')}_{step.get('title')}", {}).get("status") == "completed")
                enabled = sum(1 for j in range(len(steps)) if st.session_state.step_enabled.get(f"enabled_{i}_{j}", True))
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.progress(completed / len(steps) if steps else 0)
                    st.caption(f"Progress: {completed}/{len(steps)} completed • {enabled} enabled")
                with col2:
                    if st.button(f"❌ None", key=f"disable_all_{i}", use_container_width=True):
                        for j in range(len(steps)):
                            st.session_state.step_enabled[f"enabled_{i}_{j}"] = False
                        st.rerun()

def render_pipeline_monitor():
    """Render real-time pipeline monitoring section"""
    st.markdown('<div class="section-header">📊 Pipeline Monitor</div>', unsafe_allow_html=True)
    
    if not st.session_state.pipeline_running:
        st.info("Pipeline is not currently running. Start a stage to see real-time monitoring.")
        return
    
    # Real-time metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Status", "🔄 Running" if st.session_state.pipeline_running else "⏸️ Idle")
    
    with col2:
        # Calculate active steps
        active_steps = sum(1 for status in st.session_state.step_status.values() if status.get("status") == "running")
        st.metric("Active Steps", active_steps)
    
    with col3:
        # Calculate completed steps
        completed_steps = sum(1 for status in st.session_state.step_status.values() if status.get("status") == "completed")
        st.metric("Completed", completed_steps)
    
    with col4:
        # Calculate failed steps
        failed_steps = sum(1 for status in st.session_state.step_status.values() if status.get("status") == "failed")
        st.metric("Failed", failed_steps)
    
    # Live log viewer
    with st.expander("📝 Live Logs", expanded=True):
        log_container = st.container()
        with log_container:
            # Placeholder for live logs
            if hasattr(st.session_state, 'pipeline_logs') and st.session_state.pipeline_logs:
                for log in st.session_state.pipeline_logs[-10:]:  # Show last 10 logs
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