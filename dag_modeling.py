from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os


from A4 import final_dataset
from B1_B2 import train_and_validate

#python_path = os.getenv("PYSPARK_PYTHON", "python3")

with DAG(
    dag_id='dag_modeling',
    default_args={'retries': 3},
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    description="Pipeline to collect and format idealista data"
) as dag:

    task_join_datasets = PythonOperator(
        task_id='join_datasets',
        python_callable=final_dataset,
        provide_context=True
    )

    task_train_model = PythonOperator(
        task_id='train_and_validate_model',
        python_callable=train_and_validate,
        provide_context=True
    )

    # Definir el flujo del DAG
    task_join_datasets >> task_train_model