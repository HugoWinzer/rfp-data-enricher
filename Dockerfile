# syntax=docker/dockerfile:1.4
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Minimal system deps (compilers for some wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first for better caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# App code
COPY src /app/src
ENV PYTHONPATH=/app

# Cloud Run listens on $PORT (defaults to 8080)
EXPOSE 8080

# KEEP THIS AS ONE LINE (no wrapping). Uses $PORT if provided by Cloud Run.
CMD exec gunicorn src.enrich_app:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --threads 8 --timeout 120
