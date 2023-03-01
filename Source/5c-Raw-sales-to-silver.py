# Databricks notebook source
# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

# params
checkpoint_version = 1
table_name_bronze = f"salesbronze{teamName}"
table_name_silver = f"salessilver{teamName}"
checkpoint_path = "checkpoints/silversales"

# COMMAND ----------

from pyspark.sql.functions import get_json_object
import time

# COMMAND ----------

# Read streaming Table: streamingdb.rawlake
df = spark.readStream.format("delta").table(f"{database_name}.{table_name_bronze}")

# COMMAND ----------

# Define (lazy) transformation
# Take the column containing the json data and map fields in json to columns we want and transform to the correct types
"""{
	"id": 375403,
	"purchasedate": "2022-07-03",
	"email": "incididunt@nulla.co.uk",
	"ip_addr": "151.113.74.1",
	"phone": "(344)-147-2498",
	"productid": 15384,
	"bonuslevel": 8,
	"location": "east",
	"area": "area8",
	"r_0": 14130000,
	"r_1": 10890000,
	"r_2": 17910000,
	"r_3": 25650000
}"""

df_transform = (
    df.select(get_json_object("body", "$.id").alias("id"),
              get_json_object("body", "$.purchasedate").cast("date").alias("purchasedate"),
              get_json_object("body", "$.email").alias("email"),
              get_json_object("body", "$.ip_addr").alias("ipaddr"),
              get_json_object("body", "$.phone").alias("phone"),
              get_json_object("body", "$.productid").cast('long').alias("productid"),
              get_json_object("body", "$.bonuslevel").cast('int').alias("bonuslevel"),
              get_json_object("body", "$.location").alias("location"),
              get_json_object("body", "$.area").alias("area"),
              get_json_object("body", "$.r_0").cast("double").alias("r_0"),
              "enqueuedTime",
              "Partition"))

# COMMAND ----------

dbutils.fs.rm(f"{lake_checkpoint_root_path}/{checkpoint_path}/v{checkpoint_version}", True)

# COMMAND ----------

# Write our streaming raw -> streaming silver(and perform the defined transformation above)
print(f"Will write to table: {database_name}.{table_name_silver}")
df_transform \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/{checkpoint_path}/v{checkpoint_version}") \
  .toTable(f"{database_name}.{table_name_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Question
# MAGIC Look in the menu to the left if you see our new salessilver table?

# COMMAND ----------

# time.sleep(30)
print(f"Table name: {database_name}.{table_name_silver}")
display(table(f"{database_name}.{table_name_silver}"))

# COMMAND ----------


