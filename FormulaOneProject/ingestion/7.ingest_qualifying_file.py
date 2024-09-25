# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying folder

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

qualifying_schema = StructType([StructField("qualifyId", IntegerType(), False),
                               StructField("raceId", IntegerType(), True),
                               StructField("driverId", IntegerType(), True),
                               StructField("constructorId", IntegerType(), True),
                               StructField("number", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("q1", StringType(), True),
                               StructField("q2", StringType(), True),
                               StructField("q3", StringType(), True)])


# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_df = (qualifying_df.withColumnRenamed("qualifyId", "qualify_id")
                .withColumnRenamed("raceId", "race_id")
                .withColumnRenamed("driverId", "driver_id")
                .withColumnRenamed("constructorId", "constructor_id")
                .withColumn("data_source", lit(v_data_source)))

# COMMAND ----------

qualifying_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

overwrite_partition(qualifying_df, 'f1_processed', 'qualifying', 'race_id')