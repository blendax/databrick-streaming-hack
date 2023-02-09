@description('Location of the keyvault')
param Location string

@description('Specifies the object ID of a user, service principal or security group in the Azure Active Directory tenant for the vault. The object ID must be unique for the list of access policies. Get it by using Get-AzADUser or Get-AzADServicePrincipal cmdlets.')
param objectId string

@description('Specifies the permissions to secrets in the vault. Valid values are: all, get, list, set, delete, backup, restore, recover, and purge.')
param secretsPermissions array = [
  'all'
]

resource akv 'Microsoft.KeyVault/vaults@2022-07-01' = {
  name: 'akv-lab-${uniqueString(resourceGroup().id)}'
  location: Location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enabledForTemplateDeployment: true
    accessPolicies: [
      {
        objectId: objectId
        tenantId: subscription().tenantId
        permissions: {
          secrets: secretsPermissions
        }
      }
    ]
  }
}

output akv_output object = akv
output akv_id string = akv.id
output akv_name string = akv.name
