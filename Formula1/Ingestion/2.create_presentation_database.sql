-- Databricks notebook source
DROP DATABASE IF EXISTS f1_presentation;
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dlml/presentation"

-- COMMAND ----------

DESC DATABASE f1_presentation
