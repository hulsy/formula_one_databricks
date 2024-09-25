# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest lap_times folder

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

lap_times_schema = StructType([StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("lap", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_df = (lap_times_df.withColumnRenamed("raceId", "race_id")
                .withColumnRenamed("driverId", "driver_id")
                .withColumn("data_source", lit(v_data_source)))

# COMMAND ----------

lap_times_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

overwrite_partition(lap_times_df, 'f1_processed', 'lap_times', 'race_id')