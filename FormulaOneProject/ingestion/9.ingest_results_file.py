# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results.json file

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col

# COMMAND ----------

result_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), False),
                                 StructField("constructorId", IntegerType(), False),
                                 StructField("number", IntegerType(), True),
                                 StructField("grid", IntegerType(), False),
                                 StructField("position", IntegerType(), True),
                                 StructField("positionText", StringType(), False),
                                 StructField("positionOrder", IntegerType(), False),
                                 StructField("points", FloatType(), False),
                                 StructField("lap", IntegerType(), False),
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 StructField("fastestLap", IntegerType(), True),
                                 StructField("rank", IntegerType(), True),
                                 StructField("fastestLapTime", StringType(), True),
                                 StructField("fastestLapSpeed", FloatType(), True),
                                 StructField("statusId", IntegerType(), False)])

# COMMAND ----------

results_df = spark.read.schema(result_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_df = (results_df.withColumnRenamed("resultId", "result_id")
                        .withColumnRenamed("raceId", "race_id")
                        .withColumnRenamed("driverId", "driver_id")
                        .withColumnRenamed("constructorId", "constructor_id")
                        .withColumnRenamed("positionText", "position_text")
                        .withColumnRenamed("positionOrder", "position_order")
                        .withColumnRenamed("fastestLap", "fastest_lap")
                        .withColumnRenamed("fastestLapTime", "fastest_lap_time")
                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
                        .withColumn("data_source", lit(v_data_source))
                        .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

results_df = results_df.drop(col("statusId"))

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

overwrite_partition(results_df, 'f1_processed', 'results', 'race_id')