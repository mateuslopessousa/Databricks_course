# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlmls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlmls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlmls.dfs.core.windows.net", "sp=racwdlmeop&st=2024-05-27T22:12:11Z&se=2024-05-28T06:12:11Z&sv=2022-11-02&sr=c&sig=PzNbkTW1Xh1qvX%2BtnGrkvAyaolsMVba9aap2vuIgfio%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlmls.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlmls.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


