import os
from urllib.request import urlopen
import json
import csv
import shutil


if not os.path.exists("Landing Zone"):
    os.makedirs("Landing Zone")

# Metadata file path
metadata_file = "metadata.json"

# Landing Zone directory
landing_zone_dir = "Landing Zone"
os.makedirs(landing_zone_dir, exist_ok=True)

# Load URLs from metadata file
with open(metadata_file, "r") as file:
    metadata = json.load(file)

for key, url in metadata.items():
    file_name = os.path.join(landing_zone_dir, f"{key}.csv")

    fileobj = urlopen(url)

    response = fileobj.read().decode('utf-8')

    data = json.loads(response)

    records = data['result']['records']

    with open(file_name, mode="w") as file:
      filewriter = csv.writer(file)

      header = records[0].keys()
      filewriter.writerow(header)

      for record in records:
        filewriter.writerow(record.values())

    print(f"Data from {key} saved to {file_name}")



# Move files from /Datasets/idealista to Landing Zone
source_dir = os.path.join("Datasets", "idealista")

for file_name in os.listdir(source_dir):
    if file_name.endswith(".json"):
        source_file = os.path.join(source_dir, file_name)
        destination_file = os.path.join(landing_zone_dir, file_name)

        shutil.move(source_file, destination_file)

print("Data from idealista folder saved to Landing Zone folder")



# Move Housing files to Landing Zone
source_dir = os.path.join("Datasets")

files = ["BCN_usat_acumulat_2020.json", "BCN_usat_acumulat_2021.json"]
for file_name in files:
  source_file = os.path.join(source_dir, file_name)
  destination_file = os.path.join(landing_zone_dir, file_name)

  shutil.move(source_file, destination_file)

print("Data saved correctly to Landing Zone folder")

