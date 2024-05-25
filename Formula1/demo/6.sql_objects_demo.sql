-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create a Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC race_results_python

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python

-- COMMAND ----------

SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo_race_results_ext_py")

-- COMMAND ----------

DROP TABLE demo.race_results_ext_py

-- COMMAND ----------

DESC EXTENDED demo_race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  circuit_location STRING,
  driver_name STRING,
  driver_number STRING,
  driver_nationality STRING,
  team STRING,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dlml/presentation/race_results_ext_sql"

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

show TABLES in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.demo_race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_sql

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  circuit_location STRING,
  driver_name STRING,
  driver_number STRING,
  driver_nationality STRING,
  team STRING,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dlml/presentation/race_results_ext_sql"

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  circuit_location STRING,
  driver_name STRING,
  driver_number STRING,
  driver_nationality STRING,
  team STRING,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dlml/presentation/race_results_ext_sql"

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------


