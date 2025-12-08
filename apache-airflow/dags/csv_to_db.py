from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

DATA_PATH = "/opt/airflow/dags/support_files/"


# schema definitions for each table
TABLE_SCHEMAS = {
    "movies": """
        CREATE TABLE IF NOT EXISTS movies (
            movieId INT,
            title TEXT,
            genres TEXT
        );
    """,
    "ratings": """
        CREATE TABLE IF NOT EXISTS ratings (
            userId INT,
            movieId INT,
            rating FLOAT,
            timestamp BIGINT
        );
    """,
    "tags": """
        CREATE TABLE IF NOT EXISTS tags (
            userId INT,
            movieId INT,
            tag TEXT,
            timestamp BIGINT
        );
    """,
    "links": """
        CREATE TABLE IF NOT EXISTS links (
            movieId INT,
            imdbId INT,
            tmdbId INT
        );
    """,
    "genome_scores": """
        CREATE TABLE IF NOT EXISTS genome_scores (
            movieId INT,
            tagId INT,
            relevance FLOAT
        );
    """,
    "genome_tags": """
        CREATE TABLE IF NOT EXISTS genome_tags (
            tagId INT,
            tag TEXT
        );
    """
}


def load_csv_to_postgres(table, filename, **context):
    hook = PostgresHook(postgres_conn_id="postgres_movie")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # create tables
    cursor.execute(TABLE_SCHEMAS[table])
    conn.commit()

    # load CSV data into the table
    filepath = os.path.join(DATA_PATH, filename)

    with open(filepath, "r") as f:
        cursor.copy_expert(
            f"COPY {table} FROM STDIN WITH CSV HEADER",
            f
        )

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="load_movielens_dataset",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
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