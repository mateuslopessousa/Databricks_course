# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_presentation.calculated_race_results
# MAGIC USING parquet
# MAGIC AS 
# MAGIC SELECT 
# MAGIC     races.race_year,
# MAGIC     constructors.name AS team_name,
# MAGIC     drivers.name AS driver_name,
# MAGIC     results.position,
# MAGIC     results.points,
# MAGIC     11 - results.position AS calculated_points
# MAGIC   FROM results
# MAGIC   JOIN drivers ON (results.driver_id = drivers.driver_id)
# MAGIC   JOIN constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC   JOIN races ON (results.race_id = races.raceId)
# MAGIC WHERE results.position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results
