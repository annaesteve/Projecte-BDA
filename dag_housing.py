from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from A2 import *
from A3 import *
from A4 import *
from metadata import user_email

spark_conf = (
    SparkSession.builder
    .appName("Housing")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(spark_conf).getOrCreate()

def collect_housing_with_param(**kwargs):
    
    ti = kwargs['ti']
    landing_zone_dir = ti.xcom_pull(task_ids='landing_zone')
    collect_housing(landing_zone_dir)

def format_housing_with_params(**kwargs):
    
    ti = kwargs['ti']
    
    paths = ti.xcom_pull(task_ids='get_paths')
    landing_zone_path, formatted_zone_path = paths
    
    format_housing(spark, landing_zone_path, formatted_zone_path)

def transform_housing_with_params(**kwargs):
    
    transform_clean_housing(spark)

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
    dag_id='dag_housing',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='@yearly',
    catchup=False,
    description="Pipeline to collect and format housing data"
) as dag:

    # Tarea de landing zone
    task_landing_zone = PythonOperator(
        task_id='landing_zone',
        python_callable=landing_zone
    )
    # Recolectar datos de housing
    task_collect_housing = PythonOperator(
        task_id='collect_housing',
        python_callable=collect_housing_with_param,
        provide_context=True
    )

    # Obtención de paths
    task_get_paths = PythonOperator(
        task_id='get_paths',
        python_callable=get_paths
    )
    # Formatear datos de housing
    task_format_housing = PythonOperator(
        task_id='format_housing',
        python_callable=format_housing_with_params,
        provide_context=True
    )

    # Transformar datos de Housing
    task_transform_housing = PythonOperator(
        task_id='transform_housing',
        python_callable=transform_housing_with_params,
        provide_context=True
    )

    # Definir el flujo del DAG
    task_landing_zone >> task_collect_housing >> task_get_paths >> task_format_housing >> task_transform_housing
