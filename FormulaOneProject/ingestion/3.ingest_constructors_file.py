# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = (spark.read
                   .schema(constructors_schema)
                   .json(f"{raw_folder_path}/{v_file_date}/constructors.json"))

# COMMAND ----------

constructors_df = constructors_df.drop("url")

# COMMAND ----------

constructors_df = (constructors_df.withColumnRenamed("constructorId", "constructor_id")
                   .withColumnRenamed("constructorRef", "constructor_ref")
                   .withColumn("data_source", lit(v_data_source))
                   .withColumn("file_date", lit(v_file_date)))

# COMMAND ----------

constructors_df = add_ingestion_date(constructors_df)

# COMMAND ----------

constructors_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")