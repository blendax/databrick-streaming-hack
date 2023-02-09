@description('Location of the storage account')
param Location string

resource eventhubnamespace 'Microsoft.EventHub/namespaces@2022-01-01-preview' = {
  name: 'ns-eventhub-lab-${uniqueString(resourceGroup().id)}'
  location: Location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: true
    maximumThroughputUnits: 10
  }
}

resource eventHubIot 'Microsoft.EventHub/namespaces/eventhubs@2022-01-01-preview' = {
  parent: eventhubnamespace
  name: 'eventhub-iot-${uniqueString(resourceGroup().id)}'
  properties: {
    messageRetentionInDays: 7
    partitionCount: 4
  }
}

resource consumerGroupIot 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2022-01-01-preview' = [for i in range(1,4) : {
  parent: eventHubIot
  name: 'databrickslab-${i}'
}]

resource eventHubSales 'Microsoft.EventHub/namespaces/eventhubs@2022-01-01-preview' = {
  parent: eventhubnamespace
  name: 'eventhub-sales-${uniqueString(resourceGroup().id)}'
  properties: {
    messageRetentionInDays: 7
    partitionCount: 4

  }
}

resource consumerGroupSales 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2022-01-01-preview' = [for i in range(1,4) : {
  parent: eventHubSales
  name: 'databrickslab-${i}'
}]

output eventhubnamespace_name string = eventhubnamespace.name
output eventHubIot_name string = eventHubIot.name
output eventHubSales_name string = eventHubSales.name
