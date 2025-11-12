# API container
FROM python:3.10-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip (avoid 24.1 resolver bug)
RUN python -m pip install --upgrade "pip>=24.2" setuptools wheel

# Copy only requirements first (better caching)
COPY src/requirements.txt /tmp/requirements.txt
COPY src/constraints-cpu.txt /tmp/constraints-cpu.txt

# Install project deps with CPU constraints
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt -c /tmp/constraints-cpu.txt

# Copy app code
COPY . /app

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "src.api.fastapi_app:app", "--host", "0.0.0.0", "--port", "8000"]