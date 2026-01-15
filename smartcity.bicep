@description('Your name to compose resource names (e.g. "dacio")')
param ownerName string

@description('Azure region to deploy all resources')
param location string = 'eastus2'

@description('The principal ID of the Databricks workspace. Provide this in the second run.')
param databricksPrincipalId string = ''

var storageAccountName = 'sasctraffic${ownerName}'
var eventHubsNamespaceName = 'ehn-smartcity-${ownerName}'
var keyVaultName = 'kv-smartcity-${ownerName}'
var databricksWorkspaceName = 'dbw-${ownerName}'
var databricksRgName = 'dbw-${ownerName}-rg'

/* -----------------------------------------------------
   STORAGE ACCOUNT
----------------------------------------------------- */

resource stg 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  name: '${storageAccountName}/default'
  dependsOn: [ stg ]
}

resource container_bronze 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccountName}/default/bronze'
  dependsOn: [ blobService ]
}

resource container_silver 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccountName}/default/silver'
  dependsOn: [ blobService ]
}

resource container_gold 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccountName}/default/gold'
  dependsOn: [ blobService ]
}

resource container_checkpoints 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccountName}/default/checkpoints'
  dependsOn: [ blobService ]
}

var storageKeys = listKeys(stg.id, '2023-01-01')
var primaryStorageKey = storageKeys.keys[0].value

/* -----------------------------------------------------
   EVENT HUBS
----------------------------------------------------- */

resource ehn 'Microsoft.EventHub/namespaces@2022-10-01-preview' = {
  name: eventHubsNamespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
}

resource ehAuth 'Microsoft.EventHub/namespaces/AuthorizationRules@2022-10-01-preview' = {
  name: '${eventHubsNamespaceName}/RootManageSharedAccessKey'
  dependsOn: [ ehn ]
  properties: {
    rights: [
      'Listen'
      'Send'
      'Manage'
    ]
  }
}

resource eh_traffic 'Microsoft.EventHub/namespaces/eventhubs@2022-10-01-preview' = {
  name: '${eventHubsNamespaceName}/traffic-sensors'
  dependsOn: [ ehn ]
  properties: {
    messageRetentionInDays: 1
    partitionCount: 4
  }
}

resource eh_weather 'Microsoft.EventHub/namespaces/eventhubs@2022-10-01-preview' = {
  name: '${eventHubsNamespaceName}/weather-events'
  dependsOn: [ ehn ]
  properties: {
    messageRetentionInDays: 1
    partitionCount: 2
  }
}

var ehKeys = listKeys(ehAuth.id, '2022-10-01-preview')
var eventHubConnectionString = ehKeys.primaryConnectionString

/* -----------------------------------------------------
   KEY VAULT
----------------------------------------------------- */

resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: {
      name: 'standard'
      family: 'A'
    }
    enableRbacAuthorization: true
  }
}

resource kvSecretEH 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  name: '${keyVaultName}/eventhub-connection-string'
  dependsOn: [ kv, ehAuth ]
  properties: {
    value: eventHubConnectionString
  }
}

resource kvSecretStorage 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  name: '${keyVaultName}/storage-account-key'
  dependsOn: [ kv ]
  properties: {
    value: primaryStorageKey
  }
}

/* -----------------------------------------------------
   DATABRICKS WORKSPACE (CHEAPEST)
----------------------------------------------------- */

resource dbw 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksWorkspaceName
  location: location
  sku: {
    name: 'standard' // cheapest Databricks tier
  }
  properties: {
    managedResourceGroupId: '/subscriptions/${subscription().subscriptionId}/resourceGroups/${databricksRgName}'
  }
  dependsOn: [ stg ]
}

/* -----------------------------------------------------
   A CHEAP DATABRICKS CLUSTER
----------------------------------------------------- */

resource dbCluster 'Microsoft.Databricks/workspaces/clusters@2023-02-01' = {
  name: '${databricksWorkspaceName}/cheap-cluster'
  dependsOn: [ dbw ]
  properties: {
    clusterName: 'cheap-cluster'
    sparkVersion: '16.4.x-scala2.12' // LTS
    nodeTypeId: 'Standard_D4ds_v4'
    autoscale: null
    numWorkers: 1
    autoterminationMinutes: 30
  }
}

/* -----------------------------------------------------
   ROLE ASSIGNMENTS
----------------------------------------------------- */

var kvSecretsUserRoleDefinitionId = '4633458b-17de-408a-b874-0445c86b69e6'
var storageBlobDataContributorRoleDefinitionId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var azureDatabricksServicePrincipalId = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'

resource databricksOnKeyVault 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(databricksPrincipalId)) {
  name: guid(kv.id, dbw.id, kvSecretsUserRoleDefinitionId)
  scope: kv
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', kvSecretsUserRoleDefinitionId)
    principalId: databricksPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource databricksServiceOnKeyVault 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(kv.id, azureDatabricksServicePrincipalId, kvSecretsUserRoleDefinitionId)
  scope: kv
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', kvSecretsUserRoleDefinitionId)
    principalId: azureDatabricksServicePrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource databricksOnStorage 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(databricksPrincipalId)) {
  name: guid(stg.id, dbw.id, storageBlobDataContributorRoleDefinitionId)
  scope: stg
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleDefinitionId)
    principalId: databricksPrincipalId
    principalType: 'ServicePrincipal'
  }
}

/* -----------------------------------------------------
   OUTPUTS
----------------------------------------------------- */

output storageAccount string = storageAccountName
output keyVault string = keyVaultName
output eventHubNamespace string = eventHubsNamespaceName
output eventHubConnectionString_out string = eventHubConnectionString
output storageKey string = primaryStorageKey
output containers array = [
  'bronze'
  'silver'
  'gold'
  'checkpoints'
]

output databricksWorkspace string = databricksWorkspaceName
output databricksUrl string = 'https://${databricksWorkspaceName}.azuredatabricks.net'
output databricksWorkspaceId string = dbw.id
output databricksWorkspaceName_out string = databricksWorkspaceName
