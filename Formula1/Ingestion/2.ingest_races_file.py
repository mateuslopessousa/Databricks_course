# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV file using the spark datafram reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("dbfs:/mnt/formula1dlml/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 3 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("receId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestions_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data from datalake

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/formula1dlml/processed/races")
