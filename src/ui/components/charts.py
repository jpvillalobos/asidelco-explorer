"""
Chart components for Streamlit UI
"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Any


def render_pipeline_timeline(steps: List[Dict[str, Any]]):
    """Render pipeline execution timeline"""
    
    fig = go.Figure()
    
    for step in steps:
        if step.get('start_time') and step.get('end_time'):
            fig.add_trace(go.Bar(
                name=step['step_name'],
                x=[step['duration']],
                y=[step['step_name']],
                orientation='h',
                marker=dict(
                    color='green' if step['state'] == 'completed' else 'red'
                )
            ))
    
    fig.update_layout(
        title="Pipeline Execution Timeline",
        xaxis_title="Duration (seconds)",
        yaxis_title="Step",
        showlegend=False,
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_progress_gauge(percentage: float, title: str = "Progress"):
    """Render progress gauge chart"""
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=percentage,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        delta={'reference': 100},
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': "lightgray"},
                {'range': [50, 75], 'color': "gray"},
                {'range': [75, 100], 'color': "darkgray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90
            }
        }
    ))
    
    fig.update_layout(height=250)
    
    st.plotly_chart(fig, use_container_width=True)


def render_step_distribution(summary: Dict[str, Any]):
    """Render step state distribution pie chart"""
    
    states = {
        'Completed': summary.get('completed_steps', 0),
        'Failed': summary.get('failed_steps', 0),
        'Running': summary.get('running_steps', 0),
    }
    
    fig = px.pie(
        values=list(states.values()),
        names=list(states.keys()),
        title="Step Distribution",
        color_discrete_map={
            'Completed': 'green',
            'Failed': 'red',
            'Running': 'orange'
        }
    )
    
    st.plotly_chart(fig, use_container_width=True)