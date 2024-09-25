# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce Constructor Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df.filter("race_year=2020"))

# COMMAND ----------

constructor_standings_df = (race_results_df.groupBy("race_year", "team")
                            .agg(f.sum("points").alias("total_points"),
                                 f.count(f.when(f.col("position") == 1, True)).alias("wins")))

# COMMAND ----------

display(constructor_standings_df.filter("race_year=2020"))

# COMMAND ----------

team_rank_spec = Window.partitionBy("race_year").orderBy(f.desc("total_points"), f.desc("wins"))
final_df = constructor_standings_df.withColumn("rank", f.rank().over(team_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.constructor_standings")