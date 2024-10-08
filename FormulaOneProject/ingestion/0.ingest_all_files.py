# Databricks notebook source
dbutils.notebook.run("2.ingest_circuits_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("5.ingest_lap_times_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("7.ingest_qualifying_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("8.ingest_races_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("9.ingest_results_file", 0, {"p_data_source" : "Ergast API", "p_file_date": "2021-04-18"})