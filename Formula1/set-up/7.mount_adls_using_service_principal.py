# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client+id, tenant_id and client_secret from key vault
# MAGIC 1. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-id')
tenant_id = formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenant-id')
client_secret = formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlml.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

##lista os mounts
display(dbutils.fs.mounts())

# COMMAND ----------

## exclui os mounts
dbutils.fs.unmount('/mnt/formula1dl/demo')
