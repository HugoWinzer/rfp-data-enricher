# syntax=docker/dockerfile:1.4
FROM python:3.11-slim


ENV PYTHONDONTWRITEBYTECODE=1 \
PYTHONUNBUFFERED=1


WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
build-essential gcc curl && \
rm -rf /var/lib/apt/lists/*


COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
pip install --no-cache-dir -r /app/requirements.txt


COPY src /app/src
ENV PYTHONPATH=/app


EXPOSE 8080


# KEEP THIS AS ONE SINGLE LINE (no wrapping)
CMD exec gunicorn src.enrich_app:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --threads 8 --timeout 120
