FROM python:3.11-slim
WORKDIR /app
COPY src/requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt
COPY src/enrich_app.py .
ENV PORT 8080 PYTHONUNBUFFERED=1
EXPOSE 8080
CMD ["python","enrich_app.py"]

