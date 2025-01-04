import os
from urllib.request import urlopen
import json
import csv
import shutil

def landing_zone():
    if not os.path.exists("Landing Zone"):
        os.makedirs("Landing Zone")

    # Landing Zone directory
    landing_zone_dir = "Landing Zone"
    os.makedirs(landing_zone_dir, exist_ok=True)

    return landing_zone_dir

def collect_income(landing_zone_dir):
    # Metadata file path
    metadata_file = "metadata.json"

    # Load URLs from metadata file
    with open(metadata_file, "r") as file:
        metadata = json.load(file)

    for key, url in metadata.items():
        file_name = os.path.join(landing_zone_dir, f"{key}.csv")

        # needed to handle pagination in the API, if not only reads first 100 records
        offset = 0
        all_records = []
        while True:
            paginated_url = f"{url}&offset={offset}"
            fileobj = urlopen(paginated_url)
            response = fileobj.read().decode('utf-8')
            data = json.loads(response)

            # Extract records
            records = data['result']['records']
            if not records:  # Break the loop if no more records
                break

            all_records.extend(records)
            offset += len(records)

        # Save all records to CSV
        if all_records:
            with open(file_name, mode="w") as file:
                filewriter = csv.writer(file)

                # Write header
                header = all_records[0].keys()
                filewriter.writerow(header)

                # Write records
                for record in all_records:
                    filewriter.writerow(record.values())

            print(f"Data from {key} saved to {file_name}")
        else:
            print(f"No data found for {key}.")


def collect_idealista(landing_zone_dir):
    # Move files from /Datasets/idealista to Landing Zone
    source_dir = os.path.join("Datasets", "idealista")

    for file_name in os.listdir(source_dir):
        if file_name.endswith(".json"):
            source_file = os.path.join(source_dir, file_name)
            destination_file = os.path.join(landing_zone_dir, file_name)

            shutil.copy(source_file, destination_file)

    print("Data from idealista folder saved to Landing Zone folder")


def collect_housing(landing_zone_dir):
    # Move Housing files to Landing Zone
    source_dir = os.path.join("Datasets")

    files = ["BCN_usat_acumulat_2020.json", "BCN_usat_acumulat_2021.json"]
    for file_name in files:
        source_file = os.path.join(source_dir, file_name)
        destination_file = os.path.join(landing_zone_dir, file_name)

        shutil.copy(source_file, destination_file)

    print("Data saved correctly to Landing Zone folder")
