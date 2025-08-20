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
CMD ["gunicorn", "-b", ":8080",
     "--timeout", "3600", "--graceful-timeout", "3600",
     "--workers", "1", "--threads", "4", "--keep-alive", "120",
     "src.enrich_app:app"]


