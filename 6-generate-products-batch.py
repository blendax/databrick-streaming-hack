# Databricks notebook source
# MAGIC %md
# MAGIC ### Generate products dimension matching Sales stream

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

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate products dimension

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql.functions import max
import dbldatagen.distributions as dist

rows = 19001
format="parquet"
# To make sure we get ~100MB files
partiton_map = {19001: 1, 10: 4, 100: 16, 1000: 176, 10000: 1936}
country_codes = ['SE', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG', 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
country_weights = [200, 365, 67, 38, 126, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8, 17]

num_part = partiton_map.get(rows)
data_rows = rows
df_spec = (dg.DataGenerator(spark, name="product_data_set1", rows=data_rows, partitions=num_part)
                            .withIdOutput()
                            .withColumn("productid", IntegerType(), minValue=1000, maxValue=20000)
                            .withColumn("registrationdate", "date", data_range=dg.DateRange("2018-06-15 00:00:00", "2021-12-31 23:59:59", "days=1"), random=True)
                            .withColumn("updatedts", "timestamp", begin="2021-01-01 01:00:00", end="2021-12-31 23:59:00", interval="1 milliseconds", random=True)
                            .withColumn("productlevel", IntegerType(), values=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], random=True, weights=[1,2,3,4,5,7,8,3,2,1])
                            .withColumn("productname", StringType(), expr="concat('product name ', productid)", baseColumn=["productid"])
                            .withColumn("origin", StringType(), values=country_codes, weights=country_weights)
                            #.withColumn("price", "int", minValue=1, maxValue=10000, distribution=dist.Normal(mean=1000, stddev=10), random=True, baseColumn="productid")
                            .withColumn("price", "int", minValue=1, maxValue=10000, distribution=dist.Gamma(3.0,1.0), random=True)
                            .withColumn("stats", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=3)
                            )

df_products = df_spec.build()
path = f"{lake_data_root_path}/bronze/products"
print(f"Writing {rows} rows of data to: {path}")
print(f"df_products.count(){df_products.count()}")
df_products.write.mode("overwrite").saveAsTable(f"{database_name_batch}.productsbronze")

# COMMAND ----------

dbutils.notebook.exit("{" + f'"result":"products generated to:{path}"'  + "}")
