# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name").filter("circuit_id < 70")

# COMMAND ----------

races_df = (spark.read.parquet(f"{processed_folder_path}/races")
            .filter("race_year = 2019")
            .withColumnRenamed("name", "race_name"))

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = (circuits_df
                    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, 
                            races_df.race_name, races_df.round))

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Outer join

# COMMAND ----------

race_circuits_df = (circuits_df
                    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left")
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, 
                            races_df.race_name, races_df.round))

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = (circuits_df
                    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, 
                            races_df.race_name, races_df.round))

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = (circuits_df
                    .join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, 
                            races_df.race_name, races_df.round))

# COMMAND ----------

display(race_circuits_df)