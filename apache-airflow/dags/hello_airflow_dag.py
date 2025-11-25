from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# Function to be executed by the task
def hello_world():
    print("¡Hello, from Airflow!")

# Define the DAG
with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 10, 11),
    catchup=False
) as dag:

    # Define the task
    task_hello = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )

    # In this case, we only have one task
    task_hello
