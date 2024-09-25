# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregation Functions Demo

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year=2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

demo_df.select(f.count("*")).show()

# COMMAND ----------

demo_df.select(f.countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(f.sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(f.sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(f.sum("points"), f.countDistinct("race_name")).show()

# COMMAND ----------

(demo_df.filter("driver_name = 'Lewis Hamilton'").select(f.sum("points"), f.countDistinct("race_name"))
 .withColumnRenamed("sum(points)", "total_points")
 .withColumnRenamed("count(DISTINCT race_name)", "num_races")
 .show())


# COMMAND ----------

demo_df.groupBy("driver_name").sum("points").show()

# COMMAND ----------

(demo_df.groupBy("driver_name")
 .agg(f.sum("points").alias("total_points"), f.countDistinct("race_name").alias("num_races"))
 .show())


# COMMAND ----------

# MAGIC %md
# MAGIC ###Window Functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

grouped_demo_df = (demo_df.groupBy("race_year", "driver_name")
 .agg(f.sum("points").alias("total_points"), f.countDistinct("race_name").alias("num_races")))

# COMMAND ----------

display(grouped_demo_df)

# COMMAND ----------

from pyspark.sql.window import Window

driverRankSpec = Window.partitionBy("race_year").orderBy(f.desc("total_points"))
grouped_demo_df.withColumn("rank", f.rank().over(driverRankSpec)).show(100)

# COMMAND ----------

