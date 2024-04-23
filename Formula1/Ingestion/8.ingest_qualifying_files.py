# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest qualifying.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                       ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/formula1dlml/raw/qualifying/qualifying_split_*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverid and reaceid
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceID", "race_id") \
                       .withColumnRenamed("qualifyId", "qualify_id") \
                       .withColumnRenamed("constructorId", "constructor_id") \
                       .withColumn("ingestion_date", current_timestamp()) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlml/processed/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####
