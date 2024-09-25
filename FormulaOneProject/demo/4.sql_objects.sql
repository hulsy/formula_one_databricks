-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Explore spark sql

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

USE demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESC DATABASE demo

-- COMMAND ----------

DESC DATABASE EXTENDED demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC race_results_python

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT *
  FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

DESC EXTENDED race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2019

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

