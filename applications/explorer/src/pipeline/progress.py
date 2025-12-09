"""
Progress Tracking System
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, Callable, List
import logging

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of progress events"""
    PIPELINE_START = "pipeline_start"
    PIPELINE_COMPLETE = "pipeline_complete"
    PIPELINE_FAIL = "pipeline_fail"
    STEP_START = "step_start"
    STEP_PROGRESS = "step_progress"
    STEP_COMPLETE = "step_complete"
    STEP_FAIL = "step_fail"
    LOG = "log"


class ExecutionState(Enum):
    """Pipeline execution states"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ProgressInfo:
    """Progress information"""
    current: int
    total: int
    percentage: float
    message: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ProgressEvent:
    """Progress event with timestamp"""
    event_type: EventType
    timestamp: datetime
    message: str
    step_name: Optional[str] = None
    progress: Optional[ProgressInfo] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ProgressTracker:
    """Track and report pipeline execution progress"""
    
    def __init__(self):
        self.state = ExecutionState.IDLE
        self.total_steps = 0
        self.completed_steps = 0
        self.failed_steps = 0
        self.current_step: Optional[str] = None
        self.step_progress: Dict[str, ProgressInfo] = {}
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.observers: List[Callable[[ProgressEvent], None]] = []
        self.events: List[ProgressEvent] = []
    
    def subscribe(self, observer: Callable[[ProgressEvent], None]):
        """Subscribe to progress events"""
        self.observers.append(observer)
    
    def unsubscribe(self, observer: Callable[[ProgressEvent], None]):
        """Unsubscribe from progress events"""
        if observer in self.observers:
            self.observers.remove(observer)
    
    def emit_event(
        self,
        event_type: EventType,
        message: str,
        step_name: Optional[str] = None,
        progress: Optional[ProgressInfo] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ):
        """Emit a progress event to all observers"""
        event = ProgressEvent(
            event_type=event_type,
            timestamp=datetime.now(),
            message=message,
            step_name=step_name,
            progress=progress,
            metadata=metadata,
            error=error
        )
        
        # Store event
        self.events.append(event)
        
        # Notify observers
        for observer in self.observers:
            try:
                observer(event)
            except Exception as e:
                logger.error(f"Error in progress observer: {e}")
    
    def start_pipeline(self, total_steps: int):
        """Start pipeline execution"""
        self.state = ExecutionState.RUNNING
        self.total_steps = total_steps
        self.completed_steps = 0
        self.failed_steps = 0
        self.start_time = datetime.now()
        self.end_time = None
        
        self.emit_event(
            EventType.PIPELINE_START,
            f"Pipeline started with {total_steps} steps",
            metadata={"total_steps": total_steps}
        )
        
        logger.info(f"Pipeline started: {total_steps} steps")
    
    def complete_pipeline(self):
        """Mark pipeline as completed"""
        self.state = ExecutionState.COMPLETED
        self.end_time = datetime.now()
        
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time else 0
        
        self.emit_event(
            EventType.PIPELINE_COMPLETE,
            f"Pipeline completed: {self.completed_steps}/{self.total_steps} steps",
            metadata={
                "total_steps": self.total_steps,
                "completed_steps": self.completed_steps,
                "failed_steps": self.failed_steps,
                "duration_seconds": duration
            }
        )
        
        logger.info(f"Pipeline completed in {duration:.2f}s")
    
    def fail_pipeline(self, error: str):
        """Mark pipeline as failed"""
        self.state = ExecutionState.FAILED
        self.end_time = datetime.now()
        
        self.emit_event(
            EventType.PIPELINE_FAIL,
            f"Pipeline failed: {error}",
            error=error,
            metadata={
                "completed_steps": self.completed_steps,
                "failed_steps": self.failed_steps
            }
        )
        
        logger.error(f"Pipeline failed: {error}")
    
    def start_step(self, step_name: str):
        """Start a pipeline step"""
        self.current_step = step_name
        
        self.emit_event(
            EventType.STEP_START,
            f"Starting step: {step_name}",
            step_name=step_name
        )
        
        logger.info(f"Step started: {step_name}")
    
    def complete_step(self, step_name: str):
        """Mark a step as completed"""
        self.completed_steps += 1
        
        if self.current_step == step_name:
            self.current_step = None
        
        self.emit_event(
            EventType.STEP_COMPLETE,
            f"Step completed: {step_name}",
            step_name=step_name,
            metadata={
                "completed_steps": self.completed_steps,
                "total_steps": self.total_steps
            }
        )
        
        logger.info(f"Step completed: {step_name}")
    
    def fail_step(self, step_name: str, error: str):
        """Mark a step as failed"""
        self.failed_steps += 1
        
        if self.current_step == step_name:
            self.current_step = None
        
        self.emit_event(
            EventType.STEP_FAIL,
            f"Step failed: {step_name}",
            step_name=step_name,
            error=error,
            metadata={
                "failed_steps": self.failed_steps
            }
        )
        
        logger.error(f"Step failed: {step_name} - {error}")
    
    def report_progress(
        self,
        current: int,
        total: int,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Report progress for current step"""
        percentage = (current / total * 100) if total > 0 else 0
        
        progress = ProgressInfo(
            current=current,
            total=total,
            percentage=percentage,
            message=message,
            metadata=metadata
        )
        
        if self.current_step:
            self.step_progress[self.current_step] = progress
        
        self.emit_event(
            EventType.STEP_PROGRESS,
            message or f"Progress: {current}/{total}",
            step_name=self.current_step,
            progress=progress,
            metadata=metadata
        )
    
    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary"""
        duration = None
        if self.start_time:
            end = self.end_time or datetime.now()
            duration = (end - self.start_time).total_seconds()
        
        return {
            "state": self.state.value,
            "total_steps": self.total_steps,
            "completed_steps": self.completed_steps,
            "failed_steps": self.failed_steps,
            "current_step": self.current_step,
            "duration_seconds": duration,
            "steps": {
                step_name: {
                    "current": progress.current,
                    "total": progress.total,
                    "percentage": progress.percentage,
                    "message": progress.message
                }
                for step_name, progress in self.step_progress.items()
            }
        }
    
    def get_events(self, event_type: Optional[EventType] = None) -> List[ProgressEvent]:
        """Get all events or filtered by type"""
        if event_type:
            return [e for e in self.events if e.event_type == event_type]
        return self.events.copy()
    
    def clear(self):
        """Clear all progress data"""
        self.state = ExecutionState.IDLE
        self.total_steps = 0
        self.completed_steps = 0
        self.failed_steps = 0
        self.current_step = None
        self.step_progress.clear()
        self.start_time = None
        self.end_time = None
        self.events.clear()