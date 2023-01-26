# Databricks notebook source
# Get environment settings

# COMMAND ----------

# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

# Path t write raw data in lake root folder
pathWriteRaw = "streaming/raw/sales"
# Path to write checkpoint in checkpoint root folder
checkpointpath = "checkpoints/raw/sales"
# your event hubs consumer group to use for reading eventhub (create one if you don't have)
evenHubsConsumerGroup = "databricks1"
table_name = "salesraw"
checkpoint_version = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from EventHub

# COMMAND ----------

ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhubs_con_str_sales)
ehConf['eventhubs.consumerGroup'] = evenHubsConsumerGroup

# Creating an Event Hubs Source for Streaming Queries
df = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transform data from EventHub and write to predifeined databse with defined location as a table

# COMMAND ----------

(
df.withColumn("body", df["body"].cast("string"))
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("mergeSchema", "true")
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/{checkpointpath}/v{checkpoint_version}")
  .toTable(f"{database_name}.{table_name}")
)
