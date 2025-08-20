# Dockerfile (repo root)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

# install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY src/ ./src/

EXPOSE 8080

# Gunicorn with long timeouts (container has 15m Cloud Run timeout)
CMD ["gunicorn",
     "--bind", "0.0.0.0:8080",
     "--workers", "1",
     "--threads", "8",
     "--timeout", "3600",
     "--graceful-timeout", "90",
     "--keep-alive", "120",
     "src.enrich_app:app"]
