# Databricks notebook source
# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

# Path to write raw data in lake root folder
pathWriteRaw = "streaming/raw/sales"
# Path to write checkpoint in checkpoint root folder
checkpointpath = "checkpoints/raw/sales"
# your event hubs consumer group to use for reading eventhub (create one if you don't have)
evenHubsConsumerGroup = "databricks1"
table_name = "salesbronze"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Questions
# MAGIC - Expand the green streaming symbol above. Can you see the rate of the sales stream?
# MAGIC - What is the number od sales per sec?
# MAGIC - What is batch duration to the right?
# MAGIC - Look under raw data, what is there? What is startOffset and endOffSet?
# MAGIC - So it's not really streaming, it's micro batches?
# MAGIC - If you select Data in the left menu, can you find your dtabase and your table containing the sales bronze data?

# COMMAND ----------

# Run the count and see if sales are coming in
display(spark.sql(f"select count(1) from {database_name}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do in memory analysis of the sales
# MAGIC Count sales we got the last 10 minutes. Aggregate count per 15 seconds.

# COMMAND ----------

df_streaming_sales = spark.readStream.table(f"{database_name}.{table_name}")

# COMMAND ----------

from pyspark.sql.functions import window, count
# Filter and Create a window
w = df_streaming_sales.filter("enqueuedTime > (now() - INTERVAL 10 minutes)").groupBy(window("enqueuedTime", "15 second")).agg(count("body").alias("count"))

# COMMAND ----------

query = (
  w
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("salescounts")     # salescountspermin = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Questions
# MAGIC - Why does it take time before we get any distinct keys in the streaming? I.e. before we get any results below in the sql query?
# MAGIC - What is happening?
# MAGIC - We still stream a lot of data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How many sales do we have per 15 seconds over time for the  last 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salescounts

# COMMAND ----------

# MAGIC %md
# MAGIC #### Please stop the sales count stream above to save some resources before we continue
