from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from A2 import *
from A3 import *
from A4 import *

# Configuración unificada de Spark
spark_conf = (
    SparkSession.builder
    .appName("IdealistaPrediction")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
# Crear la sesión de Spark configurada con Delta
spark = configure_spark_with_delta_pip(spark_conf).getOrCreate()


def collect_idealista_with_param(**kwargs):
    ti = kwargs['ti']
    landing_zone_dir = ti.xcom_pull(task_ids='landing_zone')
    collect_idealista(landing_zone_dir)


def format_idealista_with_params(**kwargs):
    
    ti = kwargs['ti']
    
    # Obtener parámetros necesarios desde XComs
    paths = ti.xcom_pull(task_ids='get_paths')
    landing_zone_path, formatted_zone_path = paths
    
    # Llamar a la función con los parámetros obtenidos
    format_idealista(spark, landing_zone_path, formatted_zone_path)

def transform_idealista_with_params(**kwargs):
    
    # Llamar a la función con los parámetros obtenidos
    transform_clean_idealista(spark)


with DAG(
    dag_id='dag_idealista',
    default_args={'retries': 3},
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    description="Pipeline to collect and format idealista data"
) as dag:

    # Tarea de landing zone
    task_landing_zone = PythonOperator(
        task_id='landing_zone',
        python_callable=landing_zone
    )
    # Recolectar datos de Idealista
    task_collect_idealista = PythonOperator(
        task_id='collect_idealista',
        python_callable=collect_idealista_with_param,
        provide_context=True
    )

    # Obtención de paths
    task_get_paths = PythonOperator(
        task_id='get_paths',
        python_callable=get_paths
    )
    # Formatear datos de Idealista
    task_format_idealista = PythonOperator(
        task_id='format_idealista',
        python_callable=format_idealista_with_params,
        provide_context=True
    )

    # Transformar datos de Idealista
    task_transform_idealista = PythonOperator(
        task_id='transform_idealista',
        python_callable=transform_idealista_with_params,
        provide_context=True
    )

    # Definir el flujo del DAG
    task_landing_zone >> task_collect_idealista >> task_get_paths >> task_format_idealista >> task_transform_idealista