@description('Location of the data factory')
param Location string


resource datafactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: 'adf-lab-${uniqueString(resourceGroup().id)}'
  location: Location
}
