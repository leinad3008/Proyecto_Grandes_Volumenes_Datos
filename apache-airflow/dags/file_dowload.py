from datetime import datetime
from pathlib import Path
import requests

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
logger = logging.getLogger("airflow.task")

SAVE_DIR = Path("/tmp/airflow_downloads")

@task
def get_previous_month(data_interval_start=None):
    dt = data_interval_start  # Pendulum datetime
    prev = dt.subtract(months=1)  # previous month
    return {"year": prev.year, "month": prev.month}

@task
def build_paths(date_info: dict):
    year = date_info["year"]
    month = date_info["month"]

    #year = 2020
    #month = 1

    name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{name}"
    key = f"yellow/{year}/{name}"

    return {"url": url, "file_name": name, "key": key}

@task
def download(paths: dict) -> str:
    url = paths["url"]
    file_name = paths["file_name"]

    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    dest = SAVE_DIR / file_name

    logger.info(f"Downloading from {url} ...")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    logger.info(f"Downloaded file saved to {dest}")

    return str(dest)  # path local del archivo

@task
def upload_to_s3(local_path: str, bucket: str, paths: dict, conn_id: str) -> str:
    hook = S3Hook(aws_conn_id=conn_id)

    hook.load_file(
        filename=local_path,
        key=paths["key"],
        bucket_name=bucket,
        replace=True
    )

    return local_path

@task
def cleanup_file(path: str, *args, **kwargs) -> None:
    p = Path(path)
    if p.exists():
        p.unlink()
        logger.info(f"Deleted local file {path}")
        return
    
    logger.info(f"Failed to delete local file {path}")

@dag(
    start_date=datetime(2020, 2, 1), # Start one month before the first scheduled run
    end_date=datetime(2025, 1, 1),
    schedule="@monthly",  # Execute the first day of each month
    catchup=False,
    is_paused_upon_creation=False,
    tags=["download", "s3", "minio"],
    description="Download a file from a URL and upload it to S3/MinIO"
)
def download_and_upload_s3():   
    date_info = get_previous_month()
    paths = build_paths(date_info)

    local_file = download(paths)

    finish = upload_to_s3(
        local_path=local_file,
        bucket="nyc-taxi-raw",
        paths=paths,
        conn_id="minio_s3"
    )

    cleanup_file(local_file, finish)

    trigger = TriggerDagRunOperator(
        task_id="trigger_spark_transform",
        trigger_dag_id="spark_transform",
        conf={
            "fecha": "{{ data_interval_start.strftime('%Y-%m-%d') }}",
        }
    )

    finish >> trigger

download_and_upload_s3()
