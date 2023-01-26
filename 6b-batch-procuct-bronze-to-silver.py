# Databricks notebook source
# MAGIC %run
# MAGIC ./init/setuphack

# COMMAND ----------

#Params
table_name_products_bronze = "productsbronze"
table_name_products_silver = "productssilver"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run if you need to clean up
# MAGIC -- DROP TABLE IF EXISTS batchdblake.productssilver;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATE a SILVER TABLE based on schema from bronze (but with no data)

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name_batch}.{table_name_products_silver} AS SELECT * from {database_name_batch}.{table_name_products_bronze} WHERE 1=2;")

# COMMAND ----------

from pyspark.sql.functions import col,when, expr

# COMMAND ----------

# Define (lazy) transformation
df_b = spark.table(f"{database_name_batch}.{table_name_products_bronze}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Simple transform replacing SE with Sweden and FR with France for column origin

# COMMAND ----------

df_b = df_b.withColumn("Origin", expr("CASE WHEN Origin = 'SE' THEN 'Sweden' WHEN Origin = 'FR' THEN 'France' ELSE Origin END"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a temp view to use for the merge when we are done with the transformations

# COMMAND ----------

df_b.createOrReplaceTempView("transformed_bronze_tempview")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note WHEN NOT MATCHED BY required ADB runtime 12.1+
# MAGIC USE database batchdblake;
# MAGIC 
# MAGIC MERGE INTO productssilver target USING transformed_bronze_tempview source
# MAGIC    ON target.productid = source.productid
# MAGIC    WHEN MATCHED THEN UPDATE SET *
# MAGIC    WHEN NOT MATCHED BY target THEN INSERT (Id, productid, RegistrationDate, target.updatedts, ProductLevel, ProductName, Origin, Price, status) VALUES (source.id, source.productid, source.registrationdate, source.updatedts, source.productlevel, source.productname, source.origin, source.price, 'new in silver')
# MAGIC    WHEN NOT MATCHED BY source THEN UPDATE SET target.status = 'not in bronze source anymore'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Take a look at your data
# MAGIC -- What does the merge look like?

# COMMAND ----------

# MAGIC %md
# MAGIC Go to notebbok:<br>
# MAGIC <a href="$./6a-generate-products-batch-to-bronze">6a-generate-products-batch-to-bronze</a>
# MAGIC <br>
# MAGIC and chnage the number of rows generated from 19001 to 18900<br>
# MAGIC Then re-run the merge above.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- When you merged and products where missing in the bronze source table but existed already in the silver table what does the merge now look like? status for those rows?

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from productssilver where status like 'not%'
