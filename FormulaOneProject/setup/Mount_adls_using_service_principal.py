# Databricks notebook source
client_id = dbutils.secrets.get(scope = "formulaone-scope", key = "formulaone-client-id")
tenant_id = dbutils.secrets.get(scope = "formulaone-scope", key = "formulaone-tenant-id")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="formulaone-scope",key="formulaone-client-secret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@dlalexformulaone.dfs.core.windows.net/",
  mount_point = "/mnt/dlalexformulaone/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dlalexformulaone/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/dlalexformulaone/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/dlalexformulaone/demo")