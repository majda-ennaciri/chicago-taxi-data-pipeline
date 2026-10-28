FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY taxi_pipeline ./taxi_pipeline
COPY tests ./tests
COPY dagster.yaml /opt/dagster/home/dagster.yaml
ENV DAGSTER_HOME=/opt/dagster/home PYTHONUNBUFFERED=1 SPARK_LOCAL_IP=127.0.0.1
CMD ["dagster", "dev", "-m", "taxi_pipeline.definitions", "-h", "0.0.0.0", "-p", "3000"]
