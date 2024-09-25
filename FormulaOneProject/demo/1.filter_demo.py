# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_df = races_df.filter((races_df.race_year == 2019) & (races_df.round < 9))

# COMMAND ----------

display(races_df)

# COMMAND ----------

