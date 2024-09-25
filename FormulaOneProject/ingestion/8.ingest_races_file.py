# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest races.csv file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import pyspark.sql.functions as f

# COMMAND ----------

races_schema = StructType([StructField("raceId", IntegerType(), False),
                           StructField("year", IntegerType(), True),
                           StructField("round", IntegerType(), True),
                           StructField("circuitId", IntegerType(), True),
                           StructField("name", StringType(), True),
                           StructField("date", StringType(), True),
                           StructField("time", StringType(), True),
                           StructField("url", StringType(), True)])

# COMMAND ----------

races_df = (spark.read.option("header", "true")
            .schema(races_schema)
            .csv(f"{raw_folder_path}/{v_file_date}/races.csv"))

# COMMAND ----------

races_selected_df = (races_df.select(f.col("raceId").alias("race_id"),
                                      f.col("year").alias("race_year"), "round", 
                                      f.col("circuitId").alias("circuit_id"), 
                                      "name", "date", "time"))

# COMMAND ----------

races_final_df = (races_selected_df
                  .withColumn("race_timestamp", f.to_timestamp(f.concat(f.col("date"),f.lit(" "),f.col("time")), "yyyy-MM-dd HH:mm:ss"))
                  .withColumn("data_source", lit(v_data_source))
                  .withColumn("file_date", lit(v_file_date))
                  .drop("date", "time"))

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")