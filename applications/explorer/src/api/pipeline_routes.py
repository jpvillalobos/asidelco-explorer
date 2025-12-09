"""
FastAPI routes for pipeline orchestration with progress tracking
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncio
import logging

from ..pipeline import Pipeline
from ..pipeline.steps import StepType
from ..pipeline.progress import ProgressTracker, ProgressEvent
from .websocket_routes import manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

# Global progress tracker for async broadcasting
active_pipelines: Dict[str, ProgressTracker] = {}


async def broadcast_progress(event: ProgressEvent):
    """Broadcast progress event via WebSocket"""
    await manager.broadcast(event.to_dict())


@router.post("/run-async")
async def execute_pipeline_async(request: dict, background_tasks: BackgroundTasks):
    """
    Execute pipeline asynchronously with real-time progress updates
    """
    import uuid

    pipeline_id = str(uuid.uuid4())
    progress_tracker = ProgressTracker()

    # Subscribe to progress updates for WebSocket broadcasting
    def sync_observer(event: ProgressEvent):
        # Schedule async broadcast
        asyncio.create_task(broadcast_progress(event))

    progress_tracker.subscribe(sync_observer)

    # Store tracker for status queries
    active_pipelines[pipeline_id] = progress_tracker

    def run_pipeline():
        try:
            steps = [StepType(s) for s in request.get("steps", [])]
            args = request.get("args", {})

            pipeline = Pipeline(progress_tracker=progress_tracker)
            pipeline.run(steps, args)

        except Exception as e:
            logger.error(f"Pipeline {pipeline_id} failed: {e}")
        finally:
            # Clean up after some time
            async def cleanup():
                await asyncio.sleep(300)  # Keep for 5 minutes
                active_pipelines.pop(pipeline_id, None)

            asyncio.create_task(cleanup())

    background_tasks.add_task(run_pipeline)

    return {
        "pipeline_id": pipeline_id,
        "status": "started",
        "message": "Pipeline execution started. Connect to WebSocket for progress updates.",
    }


@router.get("/status/{pipeline_id}")
async def get_pipeline_status(pipeline_id: str):
    """Get current status of a running pipeline"""
    tracker = active_pipelines.get(pipeline_id)

    if not tracker:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    return tracker.get_summary()