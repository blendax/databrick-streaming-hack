# Databricks notebook source
# Get environment settings

# COMMAND ----------

# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

table_name = "rawiot"
# Path t write raw data in lake root folder
pathWriteRaw = "streaming/rawiot"
# Path to write checkpoint in checkpoint root folder
checkpointpath = "checkpoints/rawiot"
checkpoint_version = 1

# your event hubs consumer group to use for reading eventhub (create one if you don't have)
# TODO: fill in and remove exception chose "databrickslab-1", "databrickslab-2", "databrickslab-3", "databrickslab-4" or what you have in your event hubs
# raise Exception("Fill in consumer group") 
evenHubsConsumerGroup = "databrickslab-1"

# COMMAND ----------

# Clean up folders (if needed to read from beginning)
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
  .toTable(f"{database_name}.{teamName}{table_name}")
)

# COMMAND ----------

f"{database_name}.{teamName}{table_name}"

# COMMAND ----------

# Wait until the stream above have started
display(table(f"{database_name}.{teamName}{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Look at the metadat of the table data
# MAGIC - Location
# MAGIC - Owner
# MAGIC - Type

# COMMAND ----------

display(spark.sql(f"describe formatted {database_name}.{teamName}{table_name}"))

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

f"{database_name}.{teamName}rawlake"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE financecatalog.financedb.rawlake

# COMMAND ----------

# MAGIC %md
# MAGIC ##### When we create a table form files we will not record lineage for the table

# COMMAND ----------

# Create a table of top of streaming data folder
spark.sql(f"create table IF NOT EXISTS {database_name}.{teamName}rawlake location '{lake_data_root_path}/{pathWriteRaw}'")

# COMMAND ----------

spark.sql(f"select * from {database_name}.{teamName}rawlake limit 10").show(10)

# COMMAND ----------

df_q = spark.sql(f"describe formatted {database_name}.{teamName}rawlake")
display(df_q)

# COMMAND ----------

spark.sql(f"select count(*) from {database_name}.{teamName}rawlake").show()

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
display(spark.sql(f"optimize {database_name}.{teamName}rawlake"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time travel and versions
# MAGIC Below is to show that you can work with SQL directly, but you will not be able to parameterize the objects

# COMMAND ----------

print(f"replace database (and catalog if using UC) in below scripts with: {database_name}.{teamName}{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY financedb.NoUCteam1rawiot

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from financedb.NoUCteam1rawiot version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from financedb.NoUCteam1rawiot TIMESTAMP as of '2024-02-27T14:04:19.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vaccum to get rid of obsolete files - based on default time for keeping files
# MAGIC -- On Delta tables, Databricks does not automatically trigger VACUUM operations. See Remove unused data files with vacuum: https://docs.databricks.com/delta/vacuum.html
# MAGIC vacuum financedb.NoUCteam1rawiot RETAIN 168 HOURS DRY RUN

# COMMAND ----------


