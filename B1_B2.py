import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, mean
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder

import mlflow
import mlflow.spark


def train_and_validate():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.port", "4050") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    # load the delta table from the Exploitation Zone
    cwd = os.getcwd()
    exploitation_zone = os.path.join(cwd, 'Exploitation Zone')

    original_data = spark.read.format('parquet').load(f"{exploitation_zone}/delta-table")

    # SELECTION OF THE MOST CORRELATED FEATURES

    data = original_data

    # define feature columns
    feature_columns = [
        "year", "bathrooms", "rooms", "size", "latitude", "longitude", "average_income",
        "exterior", "hasLift", "penthouse", "duplex", "studio", "chalet", "flat", "renew",
        "newdevelopment", "good", "average_price_per_m2", "price" # here we include price to calculate correlation but afterwards we will remove it
    ]

    for col_name in feature_columns:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))
        mean_value = data.select(mean(col(col_name))).first()[0]
        data = data.fillna({col_name: mean_value})  # Replace nulls with mean_value of the column

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data = assembler.transform(data)

    # we look for the features that have a higher correlation with the price
    correlation_matrix = Correlation.corr(data, "features").head()[0]

    correlation_values = correlation_matrix.toArray()[:, -1]  # Last column corresponds to 'price'
    feature_correlations = [(feature_columns[i], correlation_values[i]) for i in range(len(feature_columns))]
    sorted_features = sorted(feature_correlations, key=lambda x: abs(x[1]), reverse=True)

    print(sorted_features)

    # select features with correlation greater than a determinated threshold
    threshold = 0.1
    selected_features = [feature for feature, correlation in sorted_features if abs(correlation) > threshold and feature != "price"]

    print(f"Selected {len(selected_features)} features:", selected_features)

    assembler = VectorAssembler(inputCols=selected_features, outputCol="selected_features")
    data = assembler.transform(data)
    data_selected_features = data.select("price", "selected_features")
    data_selected_features.show(10)

    train_data, val_data = data_selected_features.randomSplit([0.8, 0.2], seed=42)

    # evaluator for RMSE
    evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")

    # initialize MLflow experiment
    mlflow.set_experiment("Reg. Models - Price Prediction")

    # RANDOM FOREST REGRESSOR
    rf = RandomForestRegressor(labelCol="price", featuresCol="selected_features")
    rf_paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [50, 100, 150]) \
        .addGrid(rf.maxDepth, [10, 15]) \
        .addGrid(rf.maxBins, [32, 64]) \
        .build()

    best_rf_model = None
    best_rf_rmse = float("inf")

    # train Random Forest with multiple hyperparameters
    for params in rf_paramGrid:
        param_values = {param.name: value for param, value in params.items()}
        run_name = f"RF_numTrees_{param_values['numTrees']}_maxDepth_{param_values['maxDepth']}_maxBins_{param_values['maxBins']}"

        with mlflow.start_run(run_name=run_name, nested=True):
            
            mlflow.log_params(param_values)
            rf.setParams(**{param.name: value for param, value in params.items()})
            
            model = rf.fit(train_data)
            predictions = model.transform(val_data)
            rmse = evaluator.evaluate(predictions)
            mlflow.log_metric("rmse", rmse)
            mlflow.spark.log_model(model, "random_forest_model")

            print(f"Random Forest - Params: {param_values}, RMSE: {rmse}")

            # update best model if applicable
            if rmse < best_rf_rmse:
                best_rf_rmse = rmse
                best_rf_model = model
                best_rf_params = param_values

    # log the best Random Forest model
    if best_rf_model:
        with mlflow.start_run(run_name="Best_RF_Model", nested=True):
            mlflow.log_params(best_rf_params)
            mlflow.log_metric("best_rmse", best_rf_rmse)
            mlflow.spark.log_model(best_rf_model, "best_random_forest_model")
            print(f"Best Random Forest Model Params: {best_rf_params}, Best RMSE: {best_rf_rmse}")

    # LINEAR REGRESSION
    lr = LinearRegression(labelCol="price", featuresCol="selected_features")
    lr_paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.1, 0.01, 0.001]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    best_lr_model = None
    best_lr_rmse = float("inf")

    # train Linear Regression with multiple hyperparameters
    for params in lr_paramGrid:
        param_values = {param.name: value for param, value in params.items()}
        run_name = f"LR_regParam_{param_values['regParam']}_elasticNet_{param_values['elasticNetParam']}"

        with mlflow.start_run(run_name=run_name, nested=True):
            
            mlflow.log_params(param_values)
            lr.setParams(**{param.name: value for param, value in params.items()})
            
            model = lr.fit(train_data)
            predictions = model.transform(val_data)
            rmse = evaluator.evaluate(predictions)
            mlflow.log_metric("rmse", rmse)
            mlflow.spark.log_model(model, "linear_regression_model")

            print(f"Linear Regression - Params: {param_values}, RMSE: {rmse}")

            # update best model if applicable
            if rmse < best_lr_rmse:
                best_lr_rmse = rmse
                best_lr_model = model
                best_lr_params = param_values

    # log the best Linear Regression model
    if best_lr_model:
        with mlflow.start_run(run_name="Best_LR_Model", nested=True):
            mlflow.log_params(best_lr_params)
            mlflow.log_metric("best_rmse", best_lr_rmse)
            mlflow.spark.log_model(best_lr_model, "best_linear_regression_model")
            print(f"Best Linear Regression Model Params: {best_lr_params}, Best RMSE: {best_lr_rmse}")



    # GRADIENT-BOOSTED TREES REGRESSOR
    gbt = GBTRegressor(labelCol="price", featuresCol="selected_features")
    gbt_paramGrid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [10, 15]) \
        .addGrid(gbt.maxBins, [32, 64]) \
        .build()

    best_gbt_model = None
    best_gbt_rmse = float("inf")

    # train GBT with multiple hyperparameters
    for params in gbt_paramGrid:
        param_values = {param.name: value for param, value in params.items()}
        run_name = f"GBT_maxDepth_{param_values['maxDepth']}_maxBins_{param_values['maxBins']}"

        with mlflow.start_run(run_name=run_name, nested=True):
            mlflow.log_params(param_values)
            gbt.setParams(**{param.name: value for param, value in params.items()})
            
            model = gbt.fit(train_data)
            predictions = model.transform(val_data)
            rmse = evaluator.evaluate(predictions)
            mlflow.log_metric("rmse", rmse)

            mlflow.spark.log_model(model, "gbt_model")

            print(f"GBT Model - Params: {param_values}, RMSE: {rmse}")

            # update best model if applicable
            if rmse < best_gbt_rmse:
                best_gbt_rmse = rmse
                best_gbt_model = model
                best_gbt_params = param_values

    # log the best GBT model
    if best_gbt_model:
        with mlflow.start_run(run_name="Best_GBT_Model", nested=True):
            mlflow.log_params(best_gbt_params)
            mlflow.log_metric("best_rmse", best_gbt_rmse)
            mlflow.spark.log_model(best_gbt_model, "best_gbt_model")
            print(f"Best GBT Model Params: {best_gbt_params}, Best RMSE: {best_gbt_rmse}")

    print("All models logged successfully with MLflow.")
