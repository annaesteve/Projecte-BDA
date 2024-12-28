import os
import sys
import json
import pyspark
import shutil
import datetime
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, when, lower

# Initialize Spark configuration and session
conf = SparkConf()
spark = SparkSession.builder \
    .config(conf=conf) \
    .master("local") \
    .appName("FlatPricePrediction") \
    .getOrCreate()

if not os.path.exists("Formatted Zone"):
    os.makedirs("Formatted Zone")

def basic_transform_idealista(df):
    """funció per realitzar transformacions bàsiques sobre df"""

    # treiem files resultat de mal format
    if "propertyCode" in df.columns:
        df = df.dropDuplicates(["propertyCode"])

    if "_corrupt_record" in df.columns:
        df = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")

    if "newDevelopmentFinished" in df.columns:
        df.drop("newDevelopmentFinished")

    if "neighborhood" in df.columns:

        # change Vallvidrera - el Tibidabo i les Planes to Vallvidrera
        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "Vallvidrera - El Tibidabo i les Planes",
                lit("Vallvidrera, el Tibidabo i les Planes")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "Vila de Gràcia",
                lit("la Vila de Gràcia")
            ).otherwise(df["neighborhood"])
        )

        # write in lowercase
        df = df.withColumn("neighborhood", lower(col("neighborhood")))

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "el poble sec - parc de montjuïc",
                lit("el poble sec - aei parc montjuïc")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "la marina del prat vermell",
                lit("la marina del prat vermell - aei zona franca")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "sant pere - santa caterina i la ribera",
                lit("sant pere, santa caterina i la ribera")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "ciutat meridiana - torre baró - vallbona",
                lit("ciutat meridiana")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "la vall d'hebron - la clota",
                lit("la vall d'hebron")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "el gòtic",
                lit("el barri gòtic")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "can peguera - el turó de la peira",
                lit("can peguera")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "sant genís dels agudells - montbau",
                lit("sant genís dels agudells")
            ).otherwise(df["neighborhood"])
        )

        df = df.withColumn(
            "neighborhood",
            when(
                df["neighborhood"] == "la marina del port",
                lit("la marina de port")
            ).otherwise(df["neighborhood"])
        )

    # AFEGIR AQUÍ TRANSFORMACIONS QUE ES NECESSITIN

    return df


from pyspark.sql.types import StructType

cwd = os.getcwd()

landing_zone_path = os.path.join(cwd, 'Landing Zone')
formatted_zone_path = f"{cwd}/Formatted Zone"

json_files_with_year = []  # Lista para guardar tuplas (file_path, year)

# Crear la lista con paths y años
for year in [2020, 2021]:
    for month in range(1, 13):
        for day in range(1, 32):
            file_path = f"{landing_zone_path}/{year}_{month:02d}_{day:02d}_idealista.json"  # Ajustar ruta según estructura
            if os.path.exists(file_path):
                json_files_with_year.append((file_path, year))  # Añadir el archivo y el año asociado


if json_files_with_year:
    # Leer todos los esquemas y unificar
    all_schemas = []
    for file_path, _ in json_files_with_year:
        df = spark.read.json(file_path)
        all_schemas.append(df.schema)

    # Crear un esquema unificado
    unified_schema = StructType()
    for schema in all_schemas:
        for field in schema.fields:
            if field.name not in [f.name for f in unified_schema.fields]:
                unified_schema.add(field)

    # Leer los archivos JSON con el esquema unificado y añadir la columna `year`
    dfs = []
    for file_path, year in json_files_with_year:
        df = spark.read.schema(unified_schema).json(file_path).withColumn("year", lit(year))
        dfs.append(df)

    # Combinar todos los DataFrames
    idealista_combined = dfs[0]
    for df in dfs[1:]:
        idealista_combined = idealista_combined.unionByName(df, allowMissingColumns=True)

    # Transformaciones adicionales
    idealista_combined = basic_transform_idealista(idealista_combined)
    idealista_combined = idealista_combined.withColumn("_id", idealista_combined["propertyCode"])
    idealista_combined.show(truncate=False)

    # Guardar en la zona formateada
    idealista_combined.write.mode("overwrite").json(f"{formatted_zone_path}/idealista_combined")

    print(f"There are {len(idealista_combined.columns)} columns and {idealista_combined.count()} rows.")

else:
    print("No JSON files found.")

import re
from pyspark.sql.functions import lower, regexp_replace, concat, col


def extract_year(filename):
    match = re.search(r'\d{4}', filename)  # Busca un año en formato YYYY
    return int(match.group()) if match else None

housing_combined = None

for file_name in os.listdir(landing_zone_path):
    if file_name.endswith(".json") and file_name.startswith("BCN"):
        json_path = os.path.join(landing_zone_path, file_name)
        year = extract_year(file_name)

        with open(json_path, "r") as f:
            data = json.load(f)
        
        # llegir bé el json
        transformed_data = [{"neighborhood": k, **v} for k, v in data.items()]
        
        df = spark.createDataFrame(transformed_data)
        df = df.withColumn('year',lit(year))
        
        if housing_combined is None:
            housing_combined = df
        else:
            housing_combined = housing_combined.union(df)

# write neighborhood in lowercase
housing_combined = housing_combined.withColumn("neighborhood", lower(col("neighborhood")))

housing_combined = housing_combined.withColumn(
    "neighborhood",
    when(
        housing_combined["neighborhood"] == "el poble sec - parc montjuïc",
        lit("el poble sec - aei parc montjuïc")
    ).otherwise(housing_combined["neighborhood"])
)

housing_combined = housing_combined.withColumn(
    "neighborhood",
    when(
        housing_combined["neighborhood"] == "la marina del prat vermell - zona franca",
        lit("la marina del prat vermell - aei zona franca")
    ).otherwise(housing_combined["neighborhood"])
)

housing_combined = housing_combined.withColumn(
    "_id", 
    concat(lower(regexp_replace(col("neighborhood"), r"[\s]", "_")), col("year").cast("string"))
)

housing_combined = housing_combined.dropDuplicates(["_id"])


# Mostrar resultado
housing_combined.show()

housing_combined.write.mode("overwrite").json(f"{formatted_zone_path}/housing_combined")


from pyspark.sql.functions import concat

income_combined = None

for file_name in os.listdir(landing_zone_path):
    if file_name.endswith(".csv") and file_name.startswith("income"):
        csv_path = os.path.join(landing_zone_path,file_name)
        if os.path.exists(csv_path):
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            if income_combined is None:
                income_combined = df
            else:
                income_combined = income_combined.union(df)

# write neighborhood in lowercase
income_combined = income_combined.withColumn("Nom_Barri", lower(col("Nom_Barri")))

income_combined = income_combined.withColumn('_id', concat(income_combined['_id'], income_combined['Codi_Districte'], income_combined['Any']))
income_combined.write.mode("overwrite").json(f"{formatted_zone_path}/income_combined")

income_combined.show()
print(f"There are {income_combined.count()} rows.")

def upload_to_mongo(df, database_name, collection_name, df_name):
    df.write.format("mongodb") \
            .option("spark.mongodb.write.database", database_name) \
            .option("spark.mongodb.write.collection", collection_name) \
            .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
            .mode("overwrite") \
            .save()

    print(f"{df_name} written to MongoDB successfully!")

spark = SparkSession.builder \
    .appName("MongoDBConnectionTest") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.4") \
    .getOrCreate()

upload_to_mongo(income_combined, "Formatted_Zone", "income", "income_combined")
upload_to_mongo(housing_combined, "Formatted_Zone", "housing", "housing_combined")
upload_to_mongo(idealista_combined, "Formatted_Zone", "idealista", "idealista_combined")
