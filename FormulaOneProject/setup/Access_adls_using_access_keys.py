# Databricks notebook source


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlalexformulaone.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dlalexformulaone.dfs.core.windows.net/circuits.csv"))