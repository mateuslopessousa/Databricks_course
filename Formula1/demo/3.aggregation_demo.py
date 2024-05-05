# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Aggregate functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ####Built-in Aggregate functionc

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "total_points") \
.show()

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.sum("points") \
.show()
##.count("race_name") \
##.countDistinct("race_name") \
##.withColumnRenamed("sum(points)", "total_points") \
##.withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \


# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points"), countDistinct("race_name")) \
.show()
##.count("race_name") \
##.countDistinct("race_name") \
##.withColumnRenamed("sum(points)", "total_points") \
##.withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df \
.groupBy("race_year" , "driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driverRankSepc = Window.partitionBy("race_year").orderBy(desc("total_points"))
teste = demo_grouped_df.withColumn("rank", rank().over(driverRankSepc))
display(teste)

# COMMAND ----------

display()
