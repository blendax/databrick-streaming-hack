@description('Location of the storage account')
param Location string

//https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles
//var storage_blob_data_contributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')

resource storage 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: 'salab${uniqueString(resourceGroup().id)}'
  location: Location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
  }
}

output storage_name string = storage.name
