from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from metadata import user_email


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': user_email
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
    
    trigger_dag_modeling = TriggerDagRunOperator(
        task_id='trigger_dag_modeling',
        trigger_dag_id='dag_modeling',
    )
    
    [trigger_dag_housing, trigger_dag_idealista, trigger_dag_income] >> trigger_dag_modeling
