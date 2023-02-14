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
pathWriteRaw = "streaming/rawiot"
# Path to write checkpoint in checkpoint root folder
checkpointpath = "checkpoints/raw"
# your event hubs consumer group to use for reading eventhub (create one if you don't have)

# TODO: fill in and remove exception chose "databrickslab-1", "databrickslab-2", "databrickslab-3", "databrickslab-4" or what you have in your event hubs
# raise Exception("Fill in consumer group") 
evenHubsConsumerGroup = "databrickslab-1"
checkpoint_version = 1


# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS raw

# COMMAND ----------

# Clean up folders
# dbutils.fs.rm(f"{lake_checkpoint_root_path}/{checkpointpath}", True)
# dbutils.fs.rm(f"{lake_data_root_path}/{pathWriteRaw}", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from EventHub

# COMMAND ----------


ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhubs_con_str_iot)
ehConf['eventhubs.consumerGroup'] = evenHubsConsumerGroup

# Creating an Event Hubs Source for Streaming Queries
df = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transform data from EventHub and write to table

# COMMAND ----------

(
df.withColumn("body", df["body"].cast("string"))
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("mergeSchema", "true")
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/raw/v{checkpoint_version}")
  .toTable(f"{database_name}.raw")
)

# COMMAND ----------

# Wait until the stream above have started
display(table(f"{database_name}.raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Look at the location of the table data

# COMMAND ----------

display(spark.sql(f"describe formatted {database_name}.raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fan out - Write a new 2nd stream to Data Lake as delta

# COMMAND ----------

# Write another stream to a folder in the lake
(df.withColumn("body", df["body"].cast("string"))
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("mergeSchema", "true")
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/raw2_v{checkpoint_version}")
  .start(f"{lake_data_root_path}/{pathWriteRaw}")
)

# COMMAND ----------

# Create a table of top of streaming data folder
spark.sql(f"create table IF NOT EXISTS {database_name}.rawlake location '{lake_data_root_path}/{pathWriteRaw}'")

# COMMAND ----------

spark.sql(f"select * from {database_name}.rawlake limit 10").show(10)

# COMMAND ----------

df_q = spark.sql(f"describe formatted {database_name}.rawlake")
display(df_q)

# COMMAND ----------

spark.sql(f"select count(*) from {database_name}.rawlake").show()

# COMMAND ----------

spark.read.option("format", "delta").load(f"{lake_data_root_path}/{pathWriteRaw}").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize many small files
# MAGIC Manually or automatically
# MAGIC See: https://docs.databricks.com/optimizations/auto-optimize.html

# COMMAND ----------

# Replace the streamingdb with your db name below
print(database_name)

# COMMAND ----------

# optimize db.table
display(spark.sql(f"optimize {database_name}.rawlake"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time travel and versions

# COMMAND ----------

print(f"replace database in below scripts with: {database_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY streamingdbdbfs.rawlake

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from streamingdbdbfs.rawlake version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from streamingdbdbfs.rawlake TIMESTAMP as of '2023-01-11T16:04:19.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vaccum to get rid of obsolete files - based on default time for keeping files
# MAGIC -- On Delta tables, Databricks does not automatically trigger VACUUM operations. See Remove unused data files with vacuum: https://docs.databricks.com/delta/vacuum.html
# MAGIC vacuum streamingdbdbfs.rawlake RETAIN 168 HOURS DRY RUN
