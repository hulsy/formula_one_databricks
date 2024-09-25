# Databricks notebook source
client_id = dbutils.secrets.get(scope = "formulaone-scope", key = "formulaone-client-id")
tenant_id = dbutils.secrets.get(scope = "formulaone-scope", key = "formulaone-tenant-id")
client_secret = dbutils.secrets.get(scope = "formulaone-scope", key = "formulaone-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlalexformulaone.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlalexformulaone.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlalexformulaone.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dlalexformulaone.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlalexformulaone.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlalexformulaone.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dlalexformulaone.dfs.core.windows.net/circuits.csv"))