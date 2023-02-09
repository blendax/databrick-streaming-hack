targetScope = 'subscription'

@description('Resource group that will contain all resources')
param ResourceGroupName string

@description('The Id of the deploying object Id')
param DeploymentObjectId string

resource rg 'Microsoft.Resources/resourceGroups@2022-09-01' existing = {
  name: ResourceGroupName
}

module adf 'modules/datafactory.bicep' = {
  scope: rg
  name: 'datafactory_deployment'
  params: {
    Location: rg.location
  }
}

module akv 'modules/azurekeyvault.bicep' = {
  scope: rg
  name: 'keyvault_deployment'
  params: {
    Location: rg.location
    objectId: DeploymentObjectId
  }
}

module storage 'modules/storageaccount.bicep' = {
  scope: rg
  name: 'storage_deployment'
  params: {
    Location: rg.location
  }
}

module databricks 'modules/databricks.bicep' = {
  scope: rg
  name: 'databricks_deployment'
  params: {
    Location: rg.location
  }
}

module eventhub 'modules/eventhub.bicep' = {
  scope: rg
  name: 'eventhub_deployment'
  params: {
    Location: rg.location
  }
}
