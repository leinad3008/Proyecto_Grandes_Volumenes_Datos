from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import json

def submit_spark_job():
    # Endpoint for Spark API REST
    spark_url = "http://spark-master:6066/v1/submissions/create"

    # Payload for the Spark job submission
    payload = {
        "action": "CreateSubmissionRequest",
        "clientSparkVersion": "4.0.1",
        "appResource": "",
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "appArgs": [ "/opt/spark-apps/test_job.py" ],
        "environmentVariables": {},
        "sparkProperties": {
            "spark.app.name": "REST_Airflow_Spark_Test",
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "cluster",
            "spark.driver.supervise": "false",
            "spark.logConf": "true",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1"
        }
    }

    headers = {"Content-Type": "application/json;charset=UTF-8"}
    response = requests.post(spark_url, data=json.dumps(payload), headers=headers)

    print("Status:", response.status_code)
    print("Response:", response.text)

    if response.status_code != 200:
        raise Exception(f"Spark submission failed: {response.text}")

with DAG(
    dag_id="spark_rest_dag",
    start_date=datetime(2025, 10, 12),
    catchup=False,
    tags=["spark", "rest"],
) as dag:
    trigger_spark = PythonOperator(
        task_id="trigger_spark_via_rest",
        python_callable=submit_spark_job
    )
