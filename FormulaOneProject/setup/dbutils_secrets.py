# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formulaone-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formulaone-scope', key='formulaone-client-id')

# COMMAND ----------

