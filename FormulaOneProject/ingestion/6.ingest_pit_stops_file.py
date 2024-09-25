# Databricks notebook source
# MAGIC %md 
# MAGIC ##Ingest pit_stops.json file

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType([StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("stop", StringType(), True),
                               StructField("lap", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("duration", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

pit_stops_df = (spark.read.schema(pit_stops_schema)
                .option("multiLine", "true")
                .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json"))

# COMMAND ----------

pit_stops_df = (pit_stops_df
                .withColumnRenamed("raceId", "race_id")
                .withColumnRenamed("driverId", "driver_id")
                .withColumn("data_source", lit(v_data_source)))

# COMMAND ----------

pit_stops_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

overwrite_partition(pit_stops_df, 'f1_processed', 'pit_stops', 'race_id')