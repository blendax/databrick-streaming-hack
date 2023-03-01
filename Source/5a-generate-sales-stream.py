# Databricks notebook source
# MAGIC %md 
# MAGIC #### Use Databricks Labs Data Generator
# MAGIC See: https://github.com/databrickslabs/dbldatagen for more info

# COMMAND ----------

try:
    import dbldatagen as dg
except:
    raise Exception("Please install PyPi lib: dbldatagen on cluster or %pip install dbldatagen in a cell in this notebook")
    

# COMMAND ----------

# Uncomment the line below if you want to install dbldatagen locally in this notebook only instead
# %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup variables and paths specific to you
# MAGIC Open the notebook in this folder: <a href="$./init/setuphack">./init/setuphack</a>

# COMMAND ----------

# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

checkpoint_version = 1
checkpoint_name = "gen_sales"

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql.functions import max, to_json, struct

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate Sales

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Build specification for data to generate

# COMMAND ----------

# Input vars
millions = 10
prefix = "generated_"
name = "sales"
postfix = "_zopt_productid_email"
format="delta"

# To make sure we get ~100MB files (also matching multiple of our cores used (88 in our case))
partiton_map = {10: 4, 100: 16, 1000: 176, 10000: 1936}
num_part = partiton_map.get(millions)
data_rows = 1000 * 1000 * millions
# spec table
df_spec = (dg.DataGenerator(spark, name="sales_data_set1", rows=data_rows, partitions=num_part)
                            .withIdOutput()
                            .withColumn("purchasedate", "date", data_range=dg.DateRange("2022-01-01 00:00:00", "2022-12-31 23:59:59", "days=1"), random=True)
                            .withColumn("email", template=r'\w.\w@\w.com|\w@\w.co.u\k')
                            .withColumn("ip_addr", template=r'\n.\n.\n.\n')
                            .withColumn("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                            .withColumn("productid", IntegerType(), minValue=1000, maxValue=20000)
                            .withColumn("bonuslevel", IntegerType(), values=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], random=True, weights=[1,1,2,4,7,7,4,2,1,1])
                            .withColumn("location", StringType(), values=['south', 'east', 'west', 'north'], random=True, weights=[5,2,2,1])
                            .withColumn("area", StringType(), values=['area1', 'area2', 'area3', 'area4', 'area5', 'area6' ,'area17', 'area8', 'area9', 'area10' ,'area11', 'area12', 'area13'], random=True, weights=[1,1,2,2,4,7,9,7,5,4,2,1,1])
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=4)
                            )                
df_gendata_stream = df_spec.build(withStreaming=True, options={'rowsPerSecond': 10})

# COMMAND ----------

# Delete any previous checkpoints so we start from scratch in the generator
dbutils.fs.rm(f"{lake_checkpoint_root_path}/checkpoints/{checkpoint_name}/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transfor the data to match expected format for EventHubs
# MAGIC We take all columns in the data frame and put them as json in the column body.<br>
# MAGIC Then we remove the other columns.

# COMMAND ----------

# Transform data
df_gendata_stream = df_gendata_stream.withColumn('body', to_json(
       struct(*df_gendata_stream.columns),
       options={"ignoreNullFields": False}))\
       .select('body')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to EventHub

# COMMAND ----------

# Write to EventHub
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhubs_con_str_sales)

df_gendata_stream \
  .writeStream \
  .format("eventhubs") \
  .outputMode("append") \
  .options(**ehConf) \
  .option("checkpointLocation", f"{lake_checkpoint_root_path}/checkpoints/{checkpoint_name}/v{checkpoint_version}") \
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Questions
# MAGIC - What purpose does the checkpoint have above when we write to EventHubs?
# MAGIC - What would happen if we removed the checkpoint (where we write to EventHubs) and re-run the notebbok?

# COMMAND ----------


