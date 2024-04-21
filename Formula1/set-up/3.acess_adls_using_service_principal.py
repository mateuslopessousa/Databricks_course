# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure Ad Application / Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-id')
tenant_id = formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenant-id')
client_secret = formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlml.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlml.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlml.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlml.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlml.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlml.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlml.dfs.core.windows.net/circuits.csv"))
