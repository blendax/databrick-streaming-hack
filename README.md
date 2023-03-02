# databrick-streaming-hack
This repo contains notebooks for 1-day lab and an end to end scenario using spark structured streaming.
Data is generated and pushed to Azure EventHubs and then read from Databricks.
Streaming from source -> bronze -> silver -> gold layer and also joining dimensinal data in the way.

The infrastructure needed for the lab can also be setup using the provided biceps templates which will also match the resource name and keys with the notebooks so that you can get started quickly. It is possible to run the lab without setting up the resources, but then you need to provide your own parameters in the setup notebook.

The lab also supports Unity Catalog which you can enable in the setuphack notebbok via a flag.
If you want to use Unity Catalog, make sue to have a Catalog and a schema/database where the users in the lab are added.
The users need rights to create and drop tables in the schema/database. The users also need a location in Unity Catalog to use where they have read and write access.

## Setup instrcutions

1. Look in the azure portal and write down the name of your storagea account to be used for the lab.
<img width="318" alt="image" src="https://user-images.githubusercontent.com/684755/215749458-a0d3e65d-b12c-424b-9041-483c9e3ee7b2.png">


2. Make sure you update your team name and the other mandatory parameters in the notebook: `/init/setuphack` this will make sure you do get your own copy of the data in the lab and don't interfere with eachothers data.
  - You are free to change parameters in `/init/setuphack` based on your needs. Don't change the variable names as the notebooks depend on them. You can though change the values of the variables.<br>
<img width="763" alt="image" src="https://user-images.githubusercontent.com/684755/222426094-3e241f1c-c9b2-4e0c-a4d7-9340e18d6e42.png">


3. you need to create a cluster
  - If you want to work as a team you can chose the following when creating a cluster (but you are free to use what you want like personal compute if you don't need to share the cluster):
    - Policy: Unrestricted
    - Single node
    - Access Mode: No isolation shared
    - Runtime version: Standard 12.1
    - Node type: Standard_DS5_V2 (56GB 16 cores)<br>
<img width="498" alt="image" src="https://user-images.githubusercontent.com/684755/215749999-983c03ba-625d-41b5-b912-9963bb78b0d5.png">
  - If you are not using Unity Catalog for the lab you ned to add storage config to the spark config so that you can access the storage account.
  - Expand the advanced options for the cluster you are creating. 
  - Copy the spark config setting in the file: `spark_config_storage.md` replace <storage-account> with the name of your storage account.<br>

<img width="677" alt="image" src="https://user-images.githubusercontent.com/684755/215750161-0236a67c-f141-4834-a057-01b751bd6e37.png"><br>
  - Add required libraries for the cluster under the library tab:
  - Maven: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
  - PyPi: `dbldatagen`<br>
<img width="521" alt="image" src="https://user-images.githubusercontent.com/684755/215750360-a3707ed1-b7b6-42a6-9a63-b8c9d0c1172e.png">
  
4. Ready to run. Run the notebooks in order staring with notebook 1.
  
Problems? Talk to your coach or solve it yourself if you don't have one :-)
  
## The notebooks and files in this lab are:

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
