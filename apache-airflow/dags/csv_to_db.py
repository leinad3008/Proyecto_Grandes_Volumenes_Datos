from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

DATA_PATH = "/opt/airflow/dags/support_files/"

def load_csv_to_postgres(table, filename, **context):
    hook = PostgresHook(postgres_conn_id="postgres-airflow")
    df = pd.read_csv(os.path.join(DATA_PATH, filename))

    # Write to Postgres (replace table if exists)
    hook.insert_rows(
        table=table,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )

with DAG(
    dag_id="load_movielens_dataset",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
):
    files = {
        "movies":          "movies.csv",
        "ratings":         "ratings.csv",
        "tags":            "tags.csv",
        "links":           "links.csv",
        "genome_scores":   "genome-scores.csv",
        "genome_tags":     "genome-tags.csv"
    }

    for table, filename in files.items():
        PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_csv_to_postgres,
            op_kwargs={"table": table, "filename": filename},
        )
