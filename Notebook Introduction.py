# Databricks notebook source
# MAGIC %md
# MAGIC #Primeiro notebook e comando mágicos
# MAGIC ##UI Introduction
# MAGIC ##Comandos mágicos
# MAGIC - Python
# MAGIC - Scala
# MAGIC - SQL
# MAGIC - Markdown
# MAGIC - File list

# COMMAND ----------

message = "Welcome to the Databricks Notebook Experiencie"

# COMMAND ----------

print(message)

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets")

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------


