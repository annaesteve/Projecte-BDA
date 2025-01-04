from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from A2 import *
from A3 import *
from time import sleep


def wait_for_dags():
    print("Waiting for the data collection DAGs to finish...")
    sleep(10*60)


with DAG(
    'dag_coordinador',
    schedule_interval='@daily',  # Ejecuta diariamente
    start_date=days_ago(1),
    catchup=False,
    description="Pipeline to run data collection, formatting, and modeling tasks"
) as dag:

    # Tareas que disparan otros DAGs
    trigger_dag_income = TriggerDagRunOperator(
        task_id='trigger_dag_income',
        trigger_dag_id='dag_income',
    )
    
    trigger_dag_housing = TriggerDagRunOperator(
        task_id='trigger_dag_housing',
        trigger_dag_id='dag_housing',
    )
    
    trigger_dag_idealista = TriggerDagRunOperator(
        task_id='trigger_dag_idealista',
        trigger_dag_id='dag_idealista',
    )

    wait_for_dags = PythonOperator(
        task_id='wait_for_dags',
        python_callable=wait_for_dags
    )
    
    
    
    # Tarea final que depende de las otras
    trigger_dag_modeling = TriggerDagRunOperator(
        task_id='trigger_dag_modeling',
        trigger_dag_id='dag_modeling',
    )
    
    
    # Una vez que los otros DAGs hayan finalizado, dispara el DAG de modelado
    [trigger_dag_housing, trigger_dag_idealista, trigger_dag_income] >> wait_for_dags >> trigger_dag_modeling
