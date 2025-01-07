from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from A2 import *
from A3 import *
from A4 import *
from metadata import user_email


spark_conf = (
    SparkSession.builder
    .appName("Income")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(spark_conf).getOrCreate()

def collect_income_with_param(**kwargs):
    ti = kwargs['ti']
    landing_zone_dir = ti.xcom_pull(task_ids='landing_zone')
    load_metadata(landing_zone_dir)

def format_income_with_params(**kwargs):
    
    conf = SparkConf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local") \
        .appName("FlatPricePrediction") \
        .getOrCreate()
    
    ti = kwargs['ti']
    
    paths = ti.xcom_pull(task_ids='get_paths')
    landing_zone_path, formatted_zone_path = paths
    
    format_income(spark, landing_zone_path, formatted_zone_path)

def transform_income_with_params(**kwargs):
    
    transform_clean_income(spark)

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
    dag_id='dag_income',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='@yearly',
    catchup=False,
    description="Pipeline to collect and format income data"
) as dag:

    task_landing_zone = PythonOperator(
        task_id='landing_zone',
        python_callable=landing_zone
    )

    task_collect_income = PythonOperator(
        task_id='collect_income',
        python_callable=collect_income_with_param,
        provide_context=True
    )

    task_get_paths = PythonOperator(
        task_id='get_paths',
        python_callable=get_paths
    )

    task_format_income = PythonOperator(
        task_id='format_income',
        python_callable=format_income_with_params,
        provide_context=True
    )

    task_transform_income = PythonOperator(
        task_id='transform_income',
        python_callable=transform_income_with_params,
        provide_context=True
    )

    task_landing_zone >> task_collect_income >> task_get_paths >> task_format_income >> task_transform_income
