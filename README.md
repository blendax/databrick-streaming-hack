# databrick-streaming-hack
This repo contains notebooks for an end to end scenario using spark structured streaming.

## Setup instrcutions

1. Look in the azure portal and write down the name of your storagea account to be used for the lab.

2. Make sure you update your team name in the notebook: `/init/setuphack` this will make sure you do get your own copy of the data in the lab.
  - You are free to change parameters in `/init/setuphack` based on your needs. Don't change the variable names as the notebooks depend on them. You can though chnage the values of the variables.

3. you need to create a cluster with a spark config so that you can access the storage account.
  - Copy the spark config setting in the file: `spark_config_storage.md` replace <storage-account> with the name of your storage account.
  - You can look at the file: `spark_config_storage.md` for what spark config to add based on how you give access to storage.
  
2. You need to set your own parameters in the: `/init/setuphack` notebook:
  - storage account name
  - your own root folder for your team
  - your own event hub consumer group
  The other parameters you can keep or chnage based on your own choices.

The notebooks and files in this lab are:

- README.md - This file
#### LAB 1

Lab one is about producing, read och writing streamning data. Also it shows the concpets of files , delta and tables and how they relate.
Some optimization and time travel is also covered.

- 1-GenerateIoTStreamData - Generate IoT data and send to EventHub
- 2-Stream-iot-to-raw - Read IoT Data from EventHub and write bronze stream to Storage 
- 3-Raw-iot-to-silver - Read bronze stream from Storage, transform stream and write silver stream to storage
- 4-Silver-iot-to-Gold - Possibility to write your own silver to Gold streaming
#### LAB 2

Lab2 is about combining streaming data and dimensional data. Sales and products. Different layers are used bronze, silver and gold.<br>
The end result is a curated dataset to be consumed in BI Tools like Power BI via Databricks SQL Warehouse.

- 5a-generate-sales-stream - Generate Sales data and send to EventHub
- 5b-Stream-sales-to-raw - Read Sales Data from EventHub and write bronze stream to table 
- 5c-Raw-sales-to-silver - Read bronze stream table, transform stream and write silver stream table
- 6a-generate-products-batch-to-bronze - Generate products and write to bronze table (batch)
- 6b-batch-procuct-bronze-to-silver - Read and transform products bronze table and write products silver table (batch)
- 7-join silver sales stream with silver products (batch) to gold - Read sales stream and read products dimension, transform, join and write joined gold layer for consumption
#### Configs
- spark_config_storage.md - exmaples of spark configs to help you access storage. Paste inte your cluster spark config under advanced.
- /init/setuphack - all parameters for the notebooks. Determing how databases and tables are named. Where data and checkpints are stored. Make sure to use these variables as thenotebooks depend on them.