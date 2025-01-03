from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from A2 import *
from A3 import *
from A4 import *

# Configuración unificada de Spark
spark_conf = (
    SparkSession.builder
    .appName("Income")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
# Crear la sesión de Spark configurada con Delta
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
    
    # Obtener parámetros necesarios desde XComs
    paths = ti.xcom_pull(task_ids='get_paths')
    landing_zone_path, formatted_zone_path = paths
    
    # Llamar a la función con los parámetros obtenidos
    format_income(spark, landing_zone_path, formatted_zone_path)

def transform_income_with_params(**kwargs):
    
    # Llamar a la función con los parámetros obtenidos
    transform_clean_income(spark)


with DAG(
    dag_id='dag_income',
    default_args={'retries': 3},
    start_date=datetime(2023, 1, 1),
    schedule='@yearly',
    catchup=False,
    description="Pipeline to collect and format income data"
) as dag:

    # Tarea de landing zone
    task_landing_zone = PythonOperator(
        task_id='landing_zone',
        python_callable=landing_zone
    )
    # Recolectar datos de income
    task_collect_income = PythonOperator(
        task_id='collect_income',
        python_callable=collect_income_with_param,
        provide_context=True
    )

    # Obtención de paths
    task_get_paths = PythonOperator(
        task_id='get_paths',
        python_callable=get_paths
    )
    # Formatear datos de income
    task_format_income = PythonOperator(
        task_id='format_income',
        python_callable=format_income_with_params,
        provide_context=True
    )

    # Transformar datos de Income
    task_transform_income = PythonOperator(
        task_id='transform_income',
        python_callable=transform_income_with_params,
        provide_context=True
    )

    # Definir el flujo del DAG
    task_landing_zone >> task_collect_income >> task_get_paths >> task_format_income >> task_transform_income
