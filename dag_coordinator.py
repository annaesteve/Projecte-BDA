from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from A2 import *
from A3 import *

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
    
    # Sensores para asegurarse de que los DAGs disparados se hayan completado
    wait_for_dag_income = ExternalTaskSensor(
        task_id='wait_for_dag_income',
        external_dag_id='dag_income',  # Nombre del DAG que se espera
        external_task_id='transform_income',  # Espera que cualquier tarea de ese DAG termine
        mode='poke',  # Se puede cambiar a 'reschedule' si prefieres otra estrategia
        timeout=600,  # Tiempo máximo de espera en segundos
        poke_interval=10  # Intervalo para comprobar si la tarea se ha completado
    )

    wait_for_dag_housing= ExternalTaskSensor(
        task_id='wait_for_dag_housing',
        external_dag_id='dag_housing',
        external_task_id='transform_housing',
        mode='poke',
        timeout=600,
        poke_interval=10
    )
    
    wait_for_dag_idealista = ExternalTaskSensor(
        task_id='wait_for_dag_idealista',
        external_dag_id='dag_idealista',
        external_task_id='transform_idealista',
        mode='poke',
        timeout=600,
        poke_interval=10
    )
    
    # Tarea final que depende de las otras
    trigger_dag_modeling = TriggerDagRunOperator(
        task_id='trigger_dag_modeling',
        trigger_dag_id='dag_modeling',
    )
    
    # Establecer las dependencias
    # Primero disparar los otros DAGs
    trigger_dag_housing >> wait_for_dag_housing
    trigger_dag_income >> wait_for_dag_income
    trigger_dag_idealista >> wait_for_dag_idealista
    
    # Una vez que los otros DAGs hayan finalizado, dispara el DAG de modelado
    [wait_for_dag_income, wait_for_dag_housing, wait_for_dag_idealista] >> trigger_dag_modeling
