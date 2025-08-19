FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

EXPOSE 8080

# Always start gunicorn; importable app is src.enrich_app:app
ENTRYPOINT ["gunicorn"]
CMD ["-b", ":8080", "src.enrich_app:app"]
