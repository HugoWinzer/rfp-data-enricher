# ---- Dockerfile ----
FROM python:3.11-slim

# Prevent python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code (note: your code is under src/)
COPY src/ ./src/

# Flask will bind to 0.0.0.0:8080 (the app uses PORT env var)
EXPOSE 8080

CMD ["python", "src/enrich_app.py"]
