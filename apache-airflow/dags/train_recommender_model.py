from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def train_model():
    """Función que entrena el modelo de recomendación"""
    from recommendation_engine import MovieRecommender
    
    recommender = MovieRecommender(
        db_host='postgres',
        db_user='postgres',
        db_password='postgres12345',
        db_name='postgresdb'
    )
    
    print("Cargando datos de la base de datos...")
    recommender.load_data()
    
    print("Entrenando modelo...")
    recommender.train_model()
    
    print("Guardando modelo...")
    recommender.save_model()
    
    print("✅ Modelo entrenado y guardado correctamente")

dag = DAG(
    'train_recommender_model',
    default_args=default_args,
    description='Entrena el modelo de recomendación de películas',
    schedule_interval='@weekly',
    start_date=datetime(2025, 12, 3),
    catchup=False,
    tags=['ml-pipeline', 'recommender'],
)

train_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)

train_task