# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

EXPOSE 8080

# Use gunicorn in production; allow Cloud Run to set $PORT
CMD ["bash", "-lc", "gunicorn -b :${PORT:-8080} src.enrich_app:app"]
