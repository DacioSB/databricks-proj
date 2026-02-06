# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Storage Gen2 to Databricks

# COMMAND ----------

# Configuration
storage_account_name = "sasctrafficdacio8"
container_names = ["bronze", "silver", "gold", "checkpoints"]
mount_point_base = "/mnt/smartcity"

# Get storage account key from Key Vault
storage_account_key = dbutils.secrets.get(scope="smartcity-secrets", key="storage-account-key")

# COMMAND ----------

def mount_container(container_name):
    """Mount a container from ADLS Gen2"""
    mount_point = f"{mount_point_base}/{container_name}"
    
    # Check if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"✓ {mount_point} already mounted")
        return
    
    # Mount configuration
    configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
    }
    
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
    
    try:
        dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✓ Successfully mounted {mount_point}")
    except Exception as e:
        print(f"✗ Error mounting {mount_point}: {str(e)}")

# COMMAND ----------

# Mount all containers
for container in container_names:
    mount_container(container)

# COMMAND ----------
#####

# Verify mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# Test write access
test_path = f"{mount_point_base}/bronze/test.txt"
dbutils.fs.put(test_path, "Mount test successful!", overwrite=True)
print(f"✓ Write test successful: {test_path}")

# Read back
content = dbutils.fs.head(test_path)
print(f"✓ Read test successful: {content}")

# Cleanup
dbutils.fs.rm(test_path)