# Databricks notebook source
# MAGIC %md
# MAGIC ## Note: 
# MAGIC #### If you want to train the model make sure you run this notebbok on a cluster that is using a recent version of the ML (Machine Learning) runtime
# MAGIC You can change the Runtime if you go to Compute -> Cluster -> Edit

# COMMAND ----------

# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

# DBTITLE 1,Prerequisite
# MAGIC %md
# MAGIC ##Train a model
# MAGIC Train a ML-model that predicts the battery voltage based on selected columns in the Silver tabel and register the model in Databricks MLflow.
# MAGIC Use the notebook provided and run it before this notebook to train and register a named model that we load.
# MAGIC Alternatively create an AutoML Experiment based on the Silver table yourself. Or why not try both?

# COMMAND ----------

import mlflow

# Registred model to load that we trained in the other notebook
model_name = f"PredictBatteryVoltage-{teamName}"
model_version = 1

# Table name for writing predictions
table_name = "silverwithpredicion"

# Path to write checkpoint in checkpoint root folder
checkpointpath = "checkpoints/silverpred"
checkpoint_version = 2

# COMMAND ----------

# DBTITLE 1,Add prediction column based on ML-Model
import mlflow
from pyspark.sql.functions import struct, col

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}/{model_version}", result_type='double')

# Read Silver table into a dataframe
dfSilver = spark.readStream.format("delta").table(f"{database_name}.{teamName}silveriot")

# Predict with loaded model on the Silver dataframe.
dfSilverWithPrediction = dfSilver.withColumn('predictions', loaded_model(struct(*map(col, dfSilver.columns)))) \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/silvertable/v{checkpoint_version}") \
  .toTable(f"{database_name}.{teamName}{table_name}")


# COMMAND ----------

display(table(f"{database_name}.{teamName}{table_name}"))
