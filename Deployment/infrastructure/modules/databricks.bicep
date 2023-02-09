@description('Location of the databricks workspace')
param Location string

resource databricks 'Microsoft.Databricks/workspaces@2022-04-01-preview' = {
  name: 'databricks-lab-${uniqueString(resourceGroup().id)}'
  location: Location
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${resourceGroup().name}-databricks-mg')
  }
}


output databricks_output object = databricks
output databricks_id string = databricks.id
