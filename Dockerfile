# Dockerfile
FROM apache/airflow:2.7.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    confluent-kafka \
    pandas \
    pyarrow \
    minio \
    boto3
