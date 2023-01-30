# Databricks notebook source
# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

checkpoint_version = 1

# COMMAND ----------

from pyspark.sql.functions import get_json_object

# COMMAND ----------

# Read streaming Table: streamingdb.rawlake
df = spark.readStream.format("delta").table(f"{database_name}.rawlake")

# COMMAND ----------

# Define (lazy) transformation - Take the column containing the json data and map fields in json to columsn we want and transform to the correct types
df_transform = df.select(get_json_object("body", "$.device_id").alias("deviceID"), \
  get_json_object("body", "$.country").alias("country"), \
  get_json_object("body", "$.model_name").alias("modelName"), \
  get_json_object("body", "$.engineRPM").cast('double').alias("engineRPM"), \
  get_json_object("body", "$.vehicleSpeed").cast('double').alias("vehicleSpeed"), \
  get_json_object("body", "$.internalBatteryVoltage").cast('double').alias("internalBatteryVoltage"), \
  get_json_object("body", "$.XAccelerometer").cast('double').alias("XAccelerometer"), \
  get_json_object("body", "$.event_ts").alias("eventTS"), \
  "enqueuedTime", \
  "Partition")

# COMMAND ----------

# Write our streaming raw -> streaming silver(and perform the defined transformation above)
df_transform \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/silvertable/v{checkpoint_version}") \
  .start(path=f"{lake_data_root_path}/silver/iot")

# COMMAND ----------

# Wait until stream above has started
display(dbutils.fs.ls(f"{lake_data_root_path}/silver/iot"))

# COMMAND ----------

df_silver = spark.read.format("delta").load(f"{lake_data_root_path}/silver/iot")

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name}.silveriot LOCATION '{lake_data_root_path}/silver/iot'")

# COMMAND ----------

display(table(f"{database_name}.silveriot"))

# COMMAND ----------

# DBTITLE 1,Add prediction column for voltage
"""import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/ea35db052e4c4895a96f04220cb91408/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')


dfSilver = spark.readStream.format("delta").table("db_lda.silvertab")


# Predict on a Spark DataFrame.
dfSilverWithPrediction = dfSilver.withColumn('predictions', loaded_model(struct(*map(col, dfSilver.columns)))) \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/eventhub/checkpoints/silvertablepred_v5") \
  .toTable("db_lda.silvertableprediction")
"""

# COMMAND ----------

"""display(table("silvertableprediction"))"""

# COMMAND ----------

"""spark.sql(f"CREATE DATABASE IF NOT EXISTS testloc LOCATION '{lake_data_root_path}/hivedw'");"""

# COMMAND ----------

"""df_transform \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/silvertable/vtable{checkpoint_version}") \
  .table("testloc.silveriot")"""

# COMMAND ----------

"""%sql
describe formatted testloc.silveriot"""

# COMMAND ----------


