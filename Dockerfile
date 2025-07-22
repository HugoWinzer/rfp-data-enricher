# Use a slim Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY src/requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/enrich_app.py .

# Expose the port the app listens on
ENV PORT 8080
EXPOSE 8080

# Run the Flask app
CMD ["python", "enrich_app.py"]
