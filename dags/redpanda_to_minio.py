from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import io
from confluent_kafka import Consumer
from minio import Minio

def stream_to_parquet():
    # 1. Connect to local MinIO
    client = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    
    # 2. Connect to Redpanda
    consumer = Consumer({
        'bootstrap.servers': 'redpanda:9092',
        'group.id': 'airflow-workers',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['raw_events'])

    # 3. Batch consume for high volume (millions/day)
    records = []
    for _ in range(10000): # Process in chunks of 10k
        msg = consumer.poll(1.0)
        if msg is None: break
        records.append(msg.value().decode('utf-8'))

    if records:
        df = pd.DataFrame(records, columns=['payload'])
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        
        # 4. Save to local storage (MinIO)
        client.put_object(
            "data-lake", 
            f"parquet/dt={datetime.now().strftime('%Y-%m-%d')}/data_{datetime.now().timestamp()}.parquet",
            data=io.BytesIO(parquet_buffer.getvalue()),
            length=len(parquet_buffer.getvalue())
        )

with DAG('high_volume_pipeline', start_date=datetime(2026, 1, 1), schedule_interval='@hourly', catchup=False) as dag:
    PythonOperator(task_id='ingest_redpanda_to_minio', python_callable=stream_to_parquet)
