import json
import requests
import time
import pendulum
from datetime import datetime

from airflow.sdk import dag, task

import logging
logger = logging.getLogger("airflow.task")

SPARK_MASTER_REST_URL = "http://spark-master:6066/v1/submissions"


def _post_json(url, payload):
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    return requests.post(url, data=json.dumps(payload), headers=headers)

@task
def get_previous_month(data_interval_start=None, dag_run=None):
    if dag_run and dag_run.conf and "fecha" in dag_run.conf:
        dt = pendulum.parse(dag_run.conf["fecha"])
    else:
        dt = data_interval_start

    prev = dt.subtract(months=1)  # previous month
    return {"year": prev.year, "month": prev.month}

@task()
def get_file_to_process(date_info: dict):
    year = date_info["year"]
    month = date_info["month"]

    #year = 2020
    #month = 1

    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"

    return {
        "input_path": f"s3a://nyc-taxi-raw/yellow",
        "output_path": f"s3a://nyc-taxi-processed/yellow",
        "file_name": file_name,
        "year": year,
        "month": month,
    }

@task()
def submit_spark_job(file_info: dict):
    file_name = file_info["file_name"]
    args = [
        file_info["input_path"],
        file_info["output_path"],
        file_name,
        file_info["year"],
        file_info["month"],
    ]

    payload = {
        "action": "CreateSubmissionRequest",
        "clientSparkVersion": "4.0.1",
        "appResource": "",
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "appArgs": [ "file:/opt/spark-apps/transform_job.py", *args],
        "environmentVariables": {},
        "sparkProperties": {
            "spark.app.name": f"Airflow_Spark_Transform_{file_name}",
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "cluster",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "spark",
            "spark.hadoop.fs.s3a.secret.key": "spark12345",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.driver.supervise": "false",
            "spark.logConf": "true",
            "spark.driver.memory": "1g",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "1",
            "spark.jars": "/opt/spark/aws/aws-sdk-bundle-2.25.53.jar",
            "spark.driver.extraClassPath": "/opt/spark/aws/aws-sdk-bundle-2.25.53.jar",
            "spark.executor.extraClassPath": "/opt/spark/aws/aws-sdk-bundle-2.25.53.jar",
        },
    }

    response = _post_json(f"{SPARK_MASTER_REST_URL}/create", payload)
    data = response.json()

    if "submissionId" not in data:
        raise Exception(f"Spark job submit failed: {data}")

    submission_id = data["submissionId"]
    logger.info(f"Spark Job ID: {submission_id}")

    return submission_id

@task()
def wait_for_completion(submission_id: str):

    status_url = f"{SPARK_MASTER_REST_URL}/status/{submission_id}"

    while True:
        resp = requests.get(status_url)
        info = resp.json()

        driver_state = info.get("driverState", "UNKNOWN")

        if driver_state == "FINISHED":
            logger.info("Spark job completed SUCCESSFULLY")
            return "SUCCESS"

        if driver_state in ("FAILED", "ERROR", "KILLED", "STOPPED"):
            raise Exception(f"Spark job FAILED with state: {driver_state}")

        time.sleep(10)


@dag(
    start_date=datetime(2020, 2, 1), # Start one month before the first scheduled run
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["spark", "transform", "s3", "minio"],
    description="Submit job to Spark Standalone via REST and wait for completion",
)
def spark_transform():
    date_info = get_previous_month()
    file_info = get_file_to_process(date_info)
    spark_id = submit_spark_job(file_info)
    wait_for_completion(spark_id)

spark_transform()