#!/bin/bash

# Run Streamlit UI
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

streamlit run src/ui/streamlit_app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true