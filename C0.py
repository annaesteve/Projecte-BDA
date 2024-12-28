import os
import shutil

# HEM DIT DE POSAR-HO A METADATA.PY

def move_to_airflow_dags():

    airflow_home = os.getenv("AIRFLOW_HOME")
    if not airflow_home:
        raise ValueError("AIRFLOW_HOME environment variable is not set.")

    # Construct the path for the scripts folder inside the AIRFLOW_HOME
    dags_path = os.path.join(airflow_home, "dags")

    if not os.path.exists(dags_path):
        os.makedirs(dags_path)

    if os.path.exists('C1_C2.py'):
        shutil.copy('C1_C2.py', dags_path)
        print(f"C1_C2.py movido a {dags_path}")
    else:
        print("'C1_C2.py' no encontrado en el directorio actual.")

move_to_airflow_dags()