I have updated the `smartcity.bicep` file. Please follow these steps to deploy your infrastructure:

**Step 1: Initial Deployment**

Run your deployment command as you normally would, without any extra parameters. This will create all the resources but will skip the role assignments for the new Databricks workspace.

Example:
```bash
az deployment group create \
  --resource-group <your-resource-group> \
  --template-file smartcity.bicep \
  --parameters ownerName=<your-owner-name>
```

**Step 2: Get the Databricks Principal ID**

After the first deployment is complete, run the following command to get the managed identity Principal ID of your new Databricks workspace. The command uses the outputs from the first deployment.

```bash
WORKSPACE_NAME=$(az deployment group show -g <your-resource-group> -n smartcity --query properties.outputs.databricksWorkspaceName_out.value -o tsv)
PRINCIPAL_ID=$(az databricks workspace show -g <your-resource-group> --name $WORKSPACE_NAME --query identity.principalId -o tsv)
echo "Your Databricks Principal ID is: $PRINCIPAL_ID"
```
*(Replace `<your-resource-group>` with your actual resource group name)*

**Step 3: Second Deployment (Assign Roles)**

Run the exact same deployment command again, but this time, pass the `databricksPrincipalId` you retrieved in the previous step as a parameter.

Example:
```bash
az deployment group create \
  --resource-group <your-resource-group> \
  --template-file smartcity.bicep \
  --parameters ownerName=<your-owner-name> databricksPrincipalId=$PRINCIPAL_ID
```

After this second run, your Databricks workspace will have the required permissions, and your automation will be complete.