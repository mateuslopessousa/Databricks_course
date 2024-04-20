# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlml.dfs.core.windows.net",
    "aQgOD/Vvjdz+Wlde54z1RgIz/bve3YPxpcQaRghFciTCwN9fSQKTov8loXlKe7veD4TaO5my7La/+ASt/RCdJQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlml.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlml.dfs.core.windows.net/circuits.csv"))
