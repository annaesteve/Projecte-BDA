import os
import sys
import pyspark
import shutil
import datetime
from delta import *

from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, when, udf
from pyspark.sql.types import ArrayType, FloatType

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array

def delta_spark_initialization():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

def transform_clean_idealista(spark):
    cwd = os.getcwd()
    exploitation_zone_clean = os.path.join(cwd, 'Exploitation Zone/clean')

    if not os.path.exists(exploitation_zone_clean):
        os.makedirs(exploitation_zone_clean)

    # Read the files saved in Formatted_Zone
    formatted_zone_path_idealista = os.path.join("Formatted Zone", "idealista_combined")
    combined_idealista_df = spark.read.json(formatted_zone_path_idealista)

    # drop rows where municipality is not Barcelona
    idealista_df = combined_idealista_df.filter(col('municipality') == 'Barcelona')

    # drop rows where operation is not sale
    idealista_df = idealista_df.filter(col('operation') == 'sale')

    # select the columns that we want to keep
    idealista_df = idealista_df.select(
        '_id', 'municipality', 'price', 'year', 'bathrooms', 'rooms', 'size', 'status', 'propertyType', 'district', 'neighborhood', 'exterior', 'hasLift', 'latitude', 'longitude'
    )

    # drop rows where propertyType is countryHouse (there is only one row with this value)
    idealista_df = idealista_df.filter(col('propertyType') != 'countryHouse')

    for column in ['propertyType', 'status']:

        # one-hot encoding for status column
        for status in idealista_df.select(column).distinct().collect():
            status_value = status[column]
            
            # create a new column for each value of status column
            idealista_df = idealista_df.withColumn(
                status_value,
                when(idealista_df[column] == status_value, lit(1)).otherwise(lit(0))
            )

    # delete the original columns
    idealista_df = idealista_df.drop('status', 'propertyType', 'municipality')

    idealista_df.show()
    print(f"In the end, there are {idealista_df.count()} rows.")

    idealista_file_path = os.path.join(exploitation_zone_clean, 'idealista_df.parquet')
    idealista_df.write.parquet(idealista_file_path, mode="overwrite")

    return idealista_df

def transform_clean_income(spark):
    cwd = os.getcwd()
    exploitation_zone_clean = os.path.join(cwd, 'Exploitation Zone/clean')

    if not os.path.exists(exploitation_zone_clean):
        os.makedirs(exploitation_zone_clean)

    # Read the files saved in Formatted_Zone
    formatted_zone_path_income = os.path.join("Formatted Zone", "income_combined")
    combined_income_df = spark.read.json(formatted_zone_path_income)

    print(f"At the beginning, there are {combined_income_df.count()} rows.")

    # rename columns
    income_df = combined_income_df.withColumnRenamed('Codi_Districte', 'district_code')
    income_df = income_df.withColumnRenamed('Nom_Districte', 'district')
    income_df = income_df.withColumnRenamed('Codi_Barri', 'neighborhood_code')
    income_df = income_df.withColumnRenamed('Nom_Barri', 'neighborhood')
    income_df = income_df.withColumnRenamed('Any', 'year')
    income_df = income_df.withColumnRenamed('Import_Euros', 'average_income')

    # group by year and neighborhood
    income_df = income_df.groupBy('year', 'district', 'neighborhood').avg('average_income')
    income_df = income_df.withColumnRenamed('avg(average_income)', 'average_income')
    income_df = income_df.withColumn('average_income', income_df['average_income'].cast('float'))
    income_df = income_df.withColumn('average_income', income_df['average_income'].cast('decimal(10, 2)'))

    # drop district_code and neighborhood_code
    income_df = income_df.drop('district_code', 'neighborhood_code', 'Seccio_Censal')

    income_df.show()
    print(f"In the end, there are {income_df.count()} rows.")

    income_file_path = os.path.join(exploitation_zone_clean, 'income_df.parquet')
    income_df.write.parquet(income_file_path, mode="overwrite")

    return income_df

def transform_clean_housing(spark):
    # Transformations and cleaning of housing

    cwd = os.getcwd()
    exploitation_zone_clean = os.path.join(cwd, 'Exploitation Zone/clean')

    if not os.path.exists(exploitation_zone_clean):
        os.makedirs(exploitation_zone_clean)

    # Read the files saved in Formatted_Zone
    formatted_zone_path_housing = os.path.join("Formatted Zone", "housing_combined")
    combined_housing_df = spark.read.json(formatted_zone_path_housing)

    print(f"At the beginning, there are {combined_housing_df.count()} rows.")

    housing_df = combined_housing_df.select('_id','neighborhood','Preu_m2_mitja', 'year')
    
    # rename column
    housing_df = housing_df.withColumnRenamed('Preu_m2_mitja', 'average_price_per_m2')

    housing_df.show()

    housing_file_path = os.path.join(exploitation_zone_clean, 'housing_df.parquet')
    housing_df.write.parquet(housing_file_path, mode="overwrite")

    return housing_df

def final_dataset():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    cwd = os.getcwd()
    exploitation_zone_clean = os.path.join(cwd, 'Exploitation Zone/clean')

    # read all three stored clean datasets, ready for joining
    income_file_path = os.path.join(exploitation_zone_clean, 'income_df.parquet')
    housing_file_path = os.path.join(exploitation_zone_clean, 'housing_df.parquet')
    idealista_file_path = os.path.join(exploitation_zone_clean, 'idealista_df.parquet')

    income_df = spark.read.parquet(income_file_path)
    housing_df = spark.read.parquet(housing_file_path)
    idealista_df = spark.read.parquet(idealista_file_path)

    # join idealista_df with average_income_df by attribute year, neighborhood
    income_df = income_df.select('year', 'neighborhood', 'average_income')
    idealista_average = idealista_df.join(income_df, ['year', 'neighborhood'], 'left')

    # join idealista_average with housing_df by attribute year, neighborhood
    housing_df = housing_df.select('year', 'neighborhood', 'average_price_per_m2')
    final_dataset = idealista_average.join(housing_df, ['year', 'neighborhood'], 'left')

    final_dataset.show()

    # write the final dataset in a delta table
    exploitation_zone = os.path.join(cwd, 'Exploitation Zone')
    path = os.path.join(exploitation_zone, 'delta-table')

    if not os.path.exists(path):
        os.makedirs(path)
        
    final_dataset.write.format("delta").partitionBy("year", "neighborhood").mode("overwrite").save(path)
    print("Final dataset saved in delta-table.")
    
