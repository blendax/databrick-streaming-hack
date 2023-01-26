# Databricks notebook source
# MAGIC %scala
# MAGIC // Test that EventHubs lib is installed on cluster
# MAGIC try {
# MAGIC   val res = Class.forName("org.apache.spark.eventhubs.ConnectionStringBuilder");
# MAGIC } catch {
# MAGIC   case e: Exception => {println("Event hub libs not installed on cluster, please install via Maven coordinate:");
# MAGIC                         println("com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22")
# MAGIC                         println("See: https://github.com/Azure/azure-event-hubs-spark")
# MAGIC                         e.printStackTrace}
# MAGIC }

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Install Databricks Labs Data Generator
# MAGIC See: https://github.com/databrickslabs/dbldatagen for more info

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup variables and paths specific to you
# MAGIC Open the notebook in this folder: <a href="$./init/setuphack">./init/setuphack</a>

# COMMAND ----------

# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

checkpoint_version = 1

# COMMAND ----------

import time
from pyspark.sql.types import LongType, IntegerType, StringType, DoubleType, FloatType
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.functions import to_json, struct, monotonically_increasing_id, rand, col, when

# COMMAND ----------

time_to_run = 180
device_population = 1000
data_rows = 1 * 10000000
partitions_requested = 4

country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']

lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

manufacturers_weights = [1300, 365, 100, 897, 1600]
country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8, 17]

manufacturers = ['VolvoTrucks', 'RenaultTrucka', 'MacTrucks', 'VolvoPenta', "VolvoBuses"]

testDataSpec = (
  dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                                 partitions=partitions_requested, randomSeedMethod='hash_fieldname',
                                 verbose=True)
                .withIdOutput()
                # we'll use hash of the base field to generate the ids to
                # avoid a simple incrementing sequence
                .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                            uniqueValues=device_population, omit=True, baseColumnType="hash")

                # note for format strings, we must use "%lx" not "%x" as the
                # underlying value is a long
                .withColumn("device_id", StringType(), format="0x%013x",
                            baseColumn="internal_device_id")

                # the device / user attributes will be the same for the same device id
                # so lets use the internal device id as the base column for these attribute
                .withColumn("country", StringType(), values=country_codes,
                            weights=country_weights,
                            baseColumn="internal_device_id")
                
                .withColumn("model_name", StringType(), values=manufacturers, weights=manufacturers_weights,
                            baseColumn="internal_device_id")
                
                # use omit = True if you don't want a column to appear in the final output
                # but just want to use it as part of generation of another column
                .withColumn("line", StringType(), values=lines, baseColumn="model_name",
                            baseColumnType="hash", omit=True)
                
                .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                            baseColumn="device_id",
                            baseColumnType="hash", omit=True)

                .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                            baseColumn=["line", "model_ser"])
                .withColumn("event_type", StringType(), 
                             values=["activation", "deactivation", "plan change", "telecoms activity", "internet activity", "device error"], random=True)
                .withColumn("engineRPM" , DoubleType(), minValue=0, maxValue=7000, distribution=dist.Gamma(1.0,2.0))
                
                .withColumn("vehicleSpeed" , DoubleType(), minValue=0.0, maxValue=162.0, step=0.1,distribution=dist.Gamma(0.1,2.7), baseColumn="engineRPM", percentNulls=0.01)
                .withColumn("internalBatteryVoltage" , FloatType(), minValue=10.0, maxValue=12.9, step=0.1, distribution=dist.Gamma(1.0,2.0))
                .withColumn("XAccelerometer" , DoubleType(), minValue=100, maxValue=200, step=0.1, distribution=dist.Gamma(1.0,2.0), percentNulls=0.02)
                
                .withColumn("event_ts", "timestamp", expr="now()")

                )

df_gendata_stream = testDataSpec.build(withStreaming=True, options={'rowsPerSecond': 100})

# COMMAND ----------

# Delete any previous checkpoints so we start from scratch in the generated data
dbutils.fs.rm(f"{lake_checkpoint_root_path}/checkpoints/gen_iot/", True)

# Transform data
df_gendata_stream = df_gendata_stream.withColumn('body', to_json(
       struct(*df_gendata_stream.columns),
       options={"ignoreNullFields": False}))\
       .select('body')
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhubs_con_str_iot)

# Write to EventHub
df_gendata_stream \
  .writeStream \
  .format("eventhubs") \
  .outputMode("append") \
  .options(**ehConf) \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/gen_iot/v{checkpoint_version}") \
  .start()
