# Use Python 3.11 slim base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY src/requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/enrich_app.py .

# Expose port and set mode
ENV PORT 8080
ENV PYTHONUNBUFFERED=1
EXPOSE 8080

# Run the service
CMD ["python", "enrich_app.py"]
