# Step 1: Use official Python image
FROM python:3.11-slim

# Step 2: Set working directory
WORKDIR /app

# Step 3: Copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Step 4: Copy all project files into container
COPY . .

# Step 5: Set environment variables (Cloud Run overrides these with secrets/envs)
ENV PORT=8080
ENV HOST=0.0.0.0

# Step 6: Expose port for Cloud Run
EXPOSE 8080

# Step 7: Run the app
CMD ["python", "enrich_app.py"]
