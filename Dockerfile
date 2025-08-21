# syntax=docker/dockerfile:1.4
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (slim + compiler for some libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first (better layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# App code
COPY src /app/src

# Let Flask/Gunicorn find the package
ENV PYTHONPATH=/app

# Cloud Run listens on $PORT (defaults to 8080)
EXPOSE 8080

# IMPORTANT: use shell form so $PORT works; keep it on **one line**
CMD exec gunicorn src.enrich_app:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --threads 8 --timeout 120
