FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY init.sh ./
COPY ingestion/ ./ingestion/
COPY anomaly_detection/ ./anomaly_detection/
COPY model_training/ ./model_training/
COPY model_deployment/ ./model_deployment/
COPY prediction/ ./prediction/

RUN chmod +x /app/init.sh

ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import prefect; print('Prefect worker healthy')" || exit 1
