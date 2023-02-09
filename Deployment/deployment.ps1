$Location = "westeurope"

$Deployment_Object_Id = (az ad signed-in-user show --query id -o tsv)

$ResourceGroupName = ""

$RandomString = (Get-Random).ToString().Substring(0,4)

if ($ResourceGroupName) {
    Write-Host "Resources will be deployed to ${ResourceGroupName}"
}
else {
    $ResourceGroupName = "rg-lab-${RandomString}"
    Write-Host "Creating resource group: ${ResourceGroupName}"
    $rg = az group create --name ${ResourceGroupName} --location $Location
}

Write-Host "Validating deployment..."
$Arm_Validate = az deployment sub validate -l westeurope -f "./infrastructure/main.bicep" --parameters ResourceGroupName=${ResourceGroupName} DeploymentObjectId=${Deployment_Object_Id} -o json
Write-Host "Done validating..."
Write-Host "Deploying resources into ${ResourceGroupName}..."
$Arm_Output = az deployment sub create -l westeurope -f "./infrastructure/main.bicep" --parameters ResourceGroupName=${ResourceGroupName} DeploymentObjectId=${Deployment_Object_Id} -o json
Write-Host "Done deploying"

Write-Host "Get deployment outputs"
$databricks_workspace_resource_id = (az deployment group show -g ${ResourceGroupName} -n databricks_deployment --query properties.outputs.databricks_id.value -o tsv)
$databricks_host = "https://$(az deployment group show -g ${ResourceGroupName} -n databricks_deployment --query properties.outputs.databricks_output.value.properties.workspaceUrl -o tsv)"
$ev_namespace_name = (az deployment group show -g ${ResourceGroupName} -n eventhub_deployment --query properties.outputs.eventhubnamespace_name.value -o tsv)
$eventhub_name_iot = (az deployment group show -g ${ResourceGroupName} -n eventhub_deployment --query properties.outputs.eventHubIot_name.value -o tsv)
$eventhub_name_sales = (az deployment group show -g ${ResourceGroupName} -n eventhub_deployment --query properties.outputs.eventHubSales_name.value -o tsv)
$keyvault_resource_id = (az deployment group show -g ${ResourceGroupName} -n keyvault_deployment --query properties.outputs.akv_id.value -o tsv)
$keyvaul_dns_name = (az deployment group show -g ${ResourceGroupName} -n keyvault_deployment --query properties.outputs.akv_output.value.properties.vaultUri -o tsv)
$keyvaul_name = (az deployment group show -g ${ResourceGroupName} -n keyvault_deployment --query properties.outputs.akv_name.value -o tsv)
$storage_name = (az deployment group show -g ${ResourceGroupName} -n storage_deployment --query properties.outputs.storage_name.value -o tsv)

Write-Host $databricks_workspace_resource_id
Write-Host $databricks_host
Write-Host $ev_namespace_name
Write-Host $eventhub_name_iot
Write-Host $eventhub_name_sales
Write-Host $ev_access_policy_name
Write-Host $keyvault_resource_id
Write-Host $keyvaul_dns_name
Write-Host $keyvaul_name
Write-Host $storage_name

Write-Host "Generate eventhub access policies..."
# Generate keys for eventhubs
$ev_iot_output = az eventhubs eventhub authorization-rule create --resource-group ${ResourceGroupName} --namespace-name ${ev_namespace_name} --eventhub-name ${eventhub_name_iot} --name iot --rights Listen Send
$ev_sales_output = az eventhubs eventhub authorization-rule create --resource-group ${ResourceGroupName} --namespace-name ${ev_namespace_name} --eventhub-name ${eventhub_name_sales} --name sales --rights Listen Send
$iot_keys =  ConvertFrom-Json "$(az eventhubs eventhub authorization-rule keys list --resource-group ${ResourceGroupName} --namespace-name ${ev_namespace_name} --eventhub-name ${eventhub_name_iot} --name iot)"
$sales_keys =  ConvertFrom-Json "$(az eventhubs eventhub authorization-rule keys list --resource-group ${ResourceGroupName} --namespace-name ${ev_namespace_name} --eventhub-name ${eventhub_name_sales} --name sales)"
# Generate storage accounts keys
$Expiry_Date = (Get-Date).AddDays(20).ToString('yyyy-MM-dd')
$StorageAccountKey = (az storage account keys list -g ${ResourceGroupName} -n ${storage_name} --query [0].value -o tsv)
$StorageSASKey = (az storage account generate-sas --account-key ${StorageAccountKey} --account-name ${storage_name} --expiry ${Expiry_Date} --https-only --permissions acdflprtuwx --resource-types sco --services bfqt)
Write-Host "Deploying secrets..."
# Create KeyVault Secrets
(az keyvault secret set --vault-name $keyvaul_name --name "IoTConnectionString" --value $iot_keys.primaryConnectionString) > $null
(az keyvault secret set --vault-name $keyvaul_name --name "SalesConnectionString" --value $sales_keys.primaryConnectionString) > $null
(az keyvault secret set --vault-name $keyvaul_name --name "StorageAccountName" --value ${storage_name}) > $null
(az keyvault secret set --vault-name $keyvaul_name --name "StorageSASToken" --value ${StorageSASKey}) > $null
(az keyvault secret set --vault-name $keyvaul_name --name "StorageAccountKey" --value ${StorageAccountKey}) > $null

Write-Host "Initialize databricks workspace..."
# Generate a token for databricks resource id
$env:DATABRICKS_AAD_TOKEN=(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query "accessToken" -o tsv)
# Generate a managment token
$management_token=(az account get-access-token --resource https://management.core.windows.net/ --query "accessToken" -o tsv)
# Configure databricks CLI
databricks configure --host "$databricks_host"  --aad-token
# Initialize databricks workspace
curl -X GET `
-H "Authorization: Bearer ${env:DATABRICKS_AAD_TOKEN}" `
-H "X-Databricks-Azure-SP-Management-Token: ${management_token}" `
-H "X-Databricks-Azure-Workspace-Resource-Id: ${databricks_workspace_resource_id}" `
$databricks_host/api/2.0/clusters/list

Write-Host "Creating azure keyvault backed secret scope..."
# Create secret scope in databricks where workspace users have access to the scope
databricks secrets create-scope --scope databricks --scope-backend-type AZURE_KEYVAULT --resource-id ${keyvault_resource_id} --dns-name ${keyvaul_dns_name} --initial-manage-principal users
Write-Host "Done..."