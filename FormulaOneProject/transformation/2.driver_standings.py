# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce Driver Standings

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

driver_standings_df = (race_results_df
                       .groupBy("race_year", "driver_name", "driver_nationality", "team")
                       .agg(f.sum("points").alias("total_points"),
                            f.count(f.when(f.col("position") == 1, True)).alias("wins")))

# COMMAND ----------

display(driver_standings_df.filter("race_year=2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(f.desc("total_points"), f.desc("wins"))
final_df = driver_standings_df.withColumn("rank", f.rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.driver_standings")