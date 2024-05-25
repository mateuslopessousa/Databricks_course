# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the csv file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
                                       ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverid and reaceid
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceID", "race_id") \
                       .withColumn("data_source", lit(v_data_source)) \
                       .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC #####
