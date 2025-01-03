import json
import os
import shutil

# Metadata file path
metadata_file = "metadata.json"

# URLs to save in metadata
income_url_2020 = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=afe5b67d-7948-4e79-a88c-d51e55fe3ac6&limit=1000"
income_url_2021 = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=e14509ca-9cba-43ec-b925-3beb5c69c2c7&limit=1000"

urls = {
    "income_url_1": income_url_2020,
    "income_url_2": income_url_2021
}

# Save metadata to a file
with open(metadata_file, "w") as file:
    json.dump(urls, file, indent=4)

print(f"Metadata saved to {metadata_file}")


def move_all_python_files_to_dags():
    """
    Move all .py files from the current working directory (CWD) 
    to the airflow/dags folder.
    """
    # Get AIRFLOW_HOME or use default ~/airflow
    airflow_home = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
    dags_folder = os.path.join(airflow_home, 'dags')

    # Ensure the dags folder exists
    os.makedirs(dags_folder, exist_ok=True)

    # List all .py files in the current working directory
    py_files = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith('.py')]

    # Move each .py file to the dags folder
    for py_file in py_files:
        src = os.path.join(os.getcwd(), py_file)
        dst = os.path.join(dags_folder, py_file)
        try:
            shutil.copy(src, dst)
            print(f"Moved {py_file} to {dags_folder}")
        except Exception as e:
            print(f"Failed to move {py_file}: {e}")


move_all_python_files_to_dags()
