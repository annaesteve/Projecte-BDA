import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Get the Python executable path from an environment variable
python_path = os.getenv("PYSPARK_PYTHON", "python")  # default to "python" if the variable is not set

# Define the DAG
with DAG(
    # C.1: Airflow code to perform scheduling of the pipelines.
    dag_id='pipeline',
    default_args={'retries': 3},
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    description="Pipeline to run data collection, formatting, and modeling tasks"
) as dag:
    spark_task_1 = BashOperator(
        task_id='collector',
        bash_command=f"{python_path} A2.py"
    )
    spark_task_2 = BashOperator(
        task_id='formatters',
        bash_command=f"{python_path} A3.py"
    )
    spark_task_3 = BashOperator(
        task_id='exploitation',
        bash_command=f"{python_path} A4.py"
    )
    spark_task_4 = BashOperator(
        task_id='model',
        bash_command=f"{python_path} B1_B2.py"
    )

    # C.2: Airflow code to create dependencies between the tasks
    spark_task_1 >> spark_task_2 >> spark_task_3 >> spark_task_4