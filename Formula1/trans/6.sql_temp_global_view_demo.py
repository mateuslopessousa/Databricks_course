# Databricks notebook source
# MAGIC %md
# MAGIC #### Global Temporary Views
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Create a global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(distinct race_name) as qtd_race from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

p_race_year = 2018

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)
