# Databricks notebook source
# MAGIC %md
# MAGIC ##### Set up params to run the notebook
# MAGIC Feel free to change these to your needs and preferences

# COMMAND ----------

use_dbfs = False

# COMMAND ----------

# Your databricks secret scope name
secret_scope = "databricks"

# Your secret name in the keyvault containing connection string to the eventhub for iot data
keyvault_secret_name_iot = "eventhubs-genstream-con-str"
eventhubs_con_str_iot = dbutils.secrets.get(secret_scope, keyvault_secret_name_iot)

# Your secret name in the keyvault containing connection string to the eventhub for seles data
keyvault_secret_name_sales = "eventhubs-gensales-con-str"
eventhubs_con_str_sales = dbutils.secrets.get(secret_scope, keyvault_secret_name_sales)


# COMMAND ----------

# Your Data root path (if you dont have data lake access you can use dbfs:/streamhackdata)
lake_data_root_path = "abfss://datasets@storagemh1westeu.dfs.core.windows.net/streamhack"
# Your checkpoints root path (if you dont have data lake access you can use dbfs:/streamhackcheckpoints)
lake_checkpoint_root_path = "abfss://process@storagemh1westeu.dfs.core.windows.net/streamhack"
# database to create or use
database_name = "streamingdblake"
# location for our database so data not end up in DBFS local storage (if you dont have data lake access you can use dbfs:/hivedw/{database_name}
database_location_hive = f"{lake_data_root_path}/hivedw/{database_name}"

# database to create or use
database_name_batch = "batchdblake"
# location for our database so data not end up in DBFS local storage (if you dont have data lake access you can use dbfs:/hivedw/{database_name}
database_location_hive_batch = f"{lake_data_root_path}/hivedw/{database_name_batch}"

# COMMAND ----------

if use_dbfs:
    # Your Data root path (if you dont have data lake access you can use dbfs:/streamhackdata)
    lake_data_root_path = "dbfs:/streamhack/data"
    # Your checkpoints root path (if you dont have data lake access you can use dbfs:/streamhackcheckpoints)
    lake_checkpoint_root_path = "dbfs:/streamhack/checkpoints"
    # database to create or use
    database_name = "streamingdbdbfs"
    # location for our database so data not end up in DBFS local storage (if you dont have data lake access you can use dbfs:/hivedw/{database_name}
    database_location_hive = "dbfs:/streamhack/hivedw"
    # database to create or use
    database_name_batch = "batchdbdbfs"
    # location for our database so data not end up in DBFS local storage (if you dont have data lake access you can use dbfs:/hivedw/{database_name}
    database_location_hive_batch = f"{lake_data_root_path}/hivedw/{database_name_batch}"

# COMMAND ----------

print("##### Using parameters ##### ")
print("lake_data_root_path:", lake_data_root_path)
print("lake_checkpoint_root_path:",lake_checkpoint_root_path)
print("secret_scope:",secret_scope)
print("keyvault_secret_name_iot:",keyvault_secret_name_iot)
print("keyvault_secret_name_sales:",keyvault_secret_name_sales)
print("database_name:", database_name)
print("database_location_hive:", database_location_hive)
print("database_name_batch:", database_name_batch)
print("database_location_hive_batch:", database_location_hive_batch)

# COMMAND ----------

# Create our own HIVE database with a specific default location
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{database_location_hive}'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name_batch} LOCATION '{database_location_hive_batch}'")

# COMMAND ----------


