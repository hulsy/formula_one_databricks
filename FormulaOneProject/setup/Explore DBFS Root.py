# Databricks notebook source
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))