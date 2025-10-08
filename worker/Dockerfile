# Dockerfile.worker

FROM python:3.10-slim
WORKDIR /app
COPY worker_batch_app.py .
RUN pip install --no-cache-dir pymongo
CMD ["python", "worker_batch_app.py"]
