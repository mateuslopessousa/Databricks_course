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


