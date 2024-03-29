# Databricks notebook source
# MAGIC %md
# MAGIC ##### Set up params to run the notebook
# MAGIC Feel free to change these to your needs and preferences.

# COMMAND ----------

# Mandatory parameters
teamName = "myteam1" # Storage Account Name
storage_account_name = "mystorageaccountname"
storage_account_container = "datasets"
use_dbfs = False # If you set use_dbfs = True you don't have to set storage_account_name
use_uc = False # Set to True if you want to use Unity Catalog (UC) in the lab
# What catalog and Schema to use in Unity Catalog to use for streaming data
catalog_name = "financecatalog" # Only needed if you use UC
database_name = "financedb" # Only need to set database_name if not using UC

if use_uc:
    database_name = f"{catalog_name}.financedb"
# What catalog and Schema in UC to use for batch data (can be the same)
database_name_batch = database_name

# COMMAND ----------

if not use_uc:
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

# COMMAND ----------

# Parameter check
if teamName == "myteam":
    raise Exception("You need to set a unique teamName above")
if not teamName.isalnum():
    raise Exception("please use only alphanumeric letters in the teamName")
if not use_dbfs and storage_account_name == "mystorageaccountname":
    raise Exception("please set the storage_account_name")

# COMMAND ----------

# If you do not have a external storage account you can use the WS local storage DBFS to simulate a storage account.
# It's not recommeneded  to store any business data in DBFS, but will work for the lab
print(f"Will use storage DBFS: {use_dbfs}")
print(f"Will use Unity Catalog: {use_uc}")

# COMMAND ----------

# Your databricks secret scope name
# See: https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope-using-the-ui to cretae a secret scopy for your secrets in a key vault
secret_scope = "databricks"

# Your secret name in the keyvault containing connection string to the eventhub for iot data
keyvault_secret_name_iot = "IoTConnectionString" #"eventhubs-genstream-con-str"
eventhubs_con_str_iot = dbutils.secrets.get(secret_scope, keyvault_secret_name_iot)

# Your secret name in the keyvault containing connection string to the eventhub for seles data
keyvault_secret_name_sales = "SalesConnectionString" #"eventhubs-gensales-con-str"
eventhubs_con_str_sales = dbutils.secrets.get(secret_scope, keyvault_secret_name_sales)

# if not use_uc:
    # key_vault_secret_name_storgae_sas_token = "StorageSASToken"
    # storage_sas_token = dbutils.secrets.get(secret_scope, key_vault_secret_name_storgae_sas_token)

key_vault_secret_name_storgae_key = "StorageAccountKey"
storage_key = dbutils.secrets.get(secret_scope, key_vault_secret_name_storgae_key)

# COMMAND ----------

# Your Data root path for the lab
lake_data_root_path = f"abfss://{storage_account_container}@{storage_account_name}.dfs.core.windows.net/streamhack/{teamName}"

# Your checkpoints root path
lake_checkpoint_root_path = f"abfss://{storage_account_container}@{storage_account_name}.dfs.core.windows.net/streamhack/{teamName}/process"

# database to create for streaming data located in the lake
database_name = f"{database_name}"

# location for our database so data not end up in DBFS WS local storage
if not use_uc:
    database_location_hive = f"{lake_data_root_path}/hivedw/{database_name}"
    
# database to create or use for batch data in the lake
database_name_batch = f"{database_name_batch}"

# location for our database so data not end up in WS DBFS local storage
if not use_uc:
    database_location_hive_batch = f"{lake_data_root_path}/hivedw/{database_name_batch}"

# COMMAND ----------

# Same parameters as above, but we will use local WS storage called DBFS instead for the lab
if use_dbfs:
    # Your Data root path for the lab
    lake_data_root_path = f"dbfs:/streamhack/data/{teamName}"
    # Your checkpoints root path
    lake_checkpoint_root_path = f"dbfs:/streamhack/checkpoints/{teamName}"
    # database to create for streaming data
    database_name = f"streamingdbdbfs{teamName}"
    # location for our database so data
    database_location_hive = f"dbfs:/streamhack/hivedw/{teamName}"
    # database to create for batch data
    database_name_batch = f"batchdbdbfs{teamName}"
    # location for our database
    database_location_hive_batch = f"{lake_data_root_path}/hivedw/{database_name_batch}"
    
if use_uc:
    # create database table if not extist in UC
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

print("##### Using parameters ##### ")
print("Team Name:", teamName)
print("Use Unity Catalog:", use_uc)
print("Use DBFS:", use_dbfs) 
print("lake_data_root_path:", lake_data_root_path)
print("lake_checkpoint_root_path:", lake_checkpoint_root_path)
print("secret_scope:", secret_scope)
print("keyvault_secret_name_iot:", keyvault_secret_name_iot)
print("keyvault_secret_name_sales:", keyvault_secret_name_sales)
print("database_name:", database_name)
print("database_name_batch:", database_name_batch)
if not use_uc:
    print("database_location_hive:", database_location_hive)
    print("database_location_hive_batch:", database_location_hive_batch)

# COMMAND ----------

# test access test
# dbutils.fs.mkdirs(lake_data_root_path)
# dbutils.fs.ls(lake_data_root_path)

# COMMAND ----------

if not use_uc:
    # Create our own HIVE database with a specific default location
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{database_location_hive}'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name_batch} LOCATION '{database_location_hive_batch}'")

# COMMAND ----------


