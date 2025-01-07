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

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': user_email,
    'on_failure_callback': task_failure_alert,
    'on_success_callback': task_success_alert
}

with DAG(
    dag_id='dag_coordinador',
    default_args=default_args
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    description="Pipeline to run data collection, formatting, and modeling tasks"
) as dag:

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
    
    trigger_dag_modeling = TriggerDagRunOperator(
        task_id='trigger_dag_modeling',
        trigger_dag_id='dag_modeling',
    )
    
    [trigger_dag_housing, trigger_dag_idealista, trigger_dag_income] >> wait_for_dags >> trigger_dag_modeling
