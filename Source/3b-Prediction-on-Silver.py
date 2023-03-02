# Databricks notebook source
# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

# DBTITLE 1,Prerequisite
# MAGIC %md
# MAGIC ##Train a model
# MAGIC Train a ML-model that predicts the battery voltage based on selected columns in the Silver tabel and register the model in Databricks MLflow. For a quick model training, create an AutoML Experiment based on the Silver table.

# COMMAND ----------

table_name = "silverwithpredicion"
logged_model = 'runs:/xxxxxxxxx/model'

# Path to write checkpoint in checkpoint root folder
checkpointpath = "checkpoints/silverpred"
checkpoint_version = 1

# COMMAND ----------

# DBTITLE 1,Add prediction column based on ML-Model
import mlflow
from pyspark.sql.functions import struct, col


# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# Read Silver table into a dataframe
dfSilver = spark.readStream.format("delta").table(f"{database_name}.silveriot")


# Predict with loaded model on the Silver dataframe.
dfSilverWithPrediction = dfSilver.withColumn('predictions', loaded_model(struct(*map(col, dfSilver.columns)))) \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/silvertable/v{checkpoint_version}") \
  .toTable(f"{database_name}.{teamName}{table_name}")


# COMMAND ----------

display(table("silvertableprediction"))"
