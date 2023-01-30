### Spark config exmample using storage key
fs.azure.account.key.<storage-account>.dfs.core.windows.net {{secrets/databrickskv/StorageAccountKey}}

### Spark config exmample using service principal to access storage account
```
fs.azure.createRemoteFileSystemDuringInitialization true
fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net OAuth
fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net https://login.microsoftonline.com/<tenantid>/oauth2/token
fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net {{secrets/scope/sp-appid}}
fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net {{secrets/scope/sp-secret}}
```

### Spark config exmample using SAS token to access storage account
```
fs.azure.createRemoteFileSystemDuringInitialization true
fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net SAS
fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider
fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net {{secrets/databricks/StorageSASToken}}
```

### Spark config exmample using SAS token to access storage account
```python
spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", "<token>")
```
