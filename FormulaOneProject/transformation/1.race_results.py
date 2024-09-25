# Databricks notebook source
# MAGIC %md
# MAGIC ##Join Race Results for Presentation Layer

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_df = (spark.table("f1_processed.results")
              .filter(f"file_date = '{v_file_date}'")
              .withColumnRenamed("time", "race_time"))

# COMMAND ----------

results_df = results_df.select(
    "result_id",
    "race_id",
    "driver_id",
    "constructor_id",
    "grid",
    "fastest_lap",
    "race_time",
    "points",
    "position",
)

# COMMAND ----------

races_df = spark.table("f1_processed.races")

# COMMAND ----------

races_df = races_df.withColumnRenamed("name", "race_name")

# COMMAND ----------

races_df = (races_df.select("race_id", "circuit_id", "race_year","race_name",
                            (to_date(col("race_timestamp"), "yyyy-MM-dd").alias("race_date"))))

# COMMAND ----------

circuits_df = spark.table("f1_processed.circuits").select(col("circuit_id"), col("location").alias("circuit_location"))

# COMMAND ----------

drivers_df = spark.table("f1_processed.drivers")

# COMMAND ----------

drivers_df = drivers_df.select("driver_id", "name", "number", "nationality")

# COMMAND ----------

drivers_df = (drivers_df.withColumnRenamed("name", "driver_name")
                        .withColumnRenamed("number", "driver_number")
                        .withColumnRenamed("nationality", "driver_nationality"))

# COMMAND ----------

constructors_df = (spark.table("f1_processed.constructors")
                   .select(col("constructor_id"), col("name").alias("team")))

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, on="circuit_id", how="inner")

# COMMAND ----------

results_joined_df = results_df.join(race_circuits_df, on="race_id", how="inner")

# COMMAND ----------

results_joined_df = results_joined_df.join(drivers_df, on="driver_id", how="inner")

# COMMAND ----------

results_joined_df = results_joined_df.join(constructors_df, on="constructor_id", how="inner")

# COMMAND ----------

results_selected_df = (results_joined_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location",
                                                "driver_name", "driver_number", "driver_nationality",
                                                "team", "grid", "fastest_lap", "race_time",
                                                "points", "position")
                                        .withColumn("created_date", current_timestamp()))

# COMMAND ----------

display(results_selected_df.filter("race_year == 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(results_selected_df.points.desc()))

# COMMAND ----------

overwrite_partition(results_selected_df, 'f1_presentation', 'race_results', 'race_id')