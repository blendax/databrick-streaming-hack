# Databricks notebook source
# MAGIC %md
# MAGIC ##### Join streaming sales with products (batch) dimension
# MAGIC Use the streaming sales table: **salessilver**<br>
# MAGIC with the products table: **productsbronze**<br>
# MAGIC <br>
# MAGIC We want to create an aggregated view based of the sales and show number of sales per product origin

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup variables and paths specific to you
# MAGIC Open the notebook in this folder: <a href="$./init/setuphack">./init/setuphack</a>

# COMMAND ----------

# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

table_name_sales = f"salessilver{teamName}"
table_name_products = f"productssilver{teamName}"
table_name_sales_product_gold = f"salesproductgold{teamName}"
checkpoint_path = "checkpoints/salesproductsjoin"
checkpoint_version = 1

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Look at Sales Silver
print(f"{database_name}.{table_name_sales}")
display(table(f"{database_name}.{table_name_sales}"))

# COMMAND ----------

display(table(f"{database_name_batch}.{table_name_products}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example sql code for join
# MAGIC -- join on productid
# MAGIC select s.id as salesid, s.productid, s.purchasedate, p.origin as productorigin, p.price as productprice,
# MAGIC   s.location as saleslocation, p.status as productstatus
# MAGIC   from streamingdblake.salessilver s
# MAGIC   LEFT OUTER join batchdblake.productssilver p ON s.productid = p.productid

# COMMAND ----------

# Read silver sales stream and do som ETL before join
df_sales_stream_silver = spark.readStream.table(f"{database_name}.{table_name_sales}")
df_sales_stream_silver = (df_sales_stream_silver.select(
    col("id").alias("salesid"),
    "productid",
    "purchasedate",
    col("location").alias("saleslocation")
))

# COMMAND ----------

# Read products (batch) and do som ETL before join
df_products_silver = spark.read.table(f"{database_name_batch}.{table_name_products}")
df_products_silver = (df_products_silver.select(
    "productid",
    col("origin").alias("productorigin"),
    col("price").alias("productprice"),
    col("status").alias("productstatus")
))

# COMMAND ----------

# Consume from Power BI via Databricks SQL
df_join_sales_prod = df_sales_stream_silver.join(df_products_silver, "productid")

# COMMAND ----------

# Write joined stream to new gold/curated table so that Power BI users and other users can use this for different aggregationsb
print(f"Writing to table: {database_name}.{table_name_sales_product_gold}")
(df_join_sales_prod.
 writeStream.
 format("delta").
 outputMode("append").
 option("mergeSchema", "true").
 option("checkpointLocation", f"{lake_checkpoint_root_path}/{checkpoint_path}/v{checkpoint_version}").
 toTable(f"{database_name}.{table_name_sales_product_gold}")
)

# COMMAND ----------


