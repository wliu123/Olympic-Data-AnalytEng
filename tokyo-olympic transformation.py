# Databricks notebook source
# clientID = dbutils.secrets.get("tokyo")
dbutils.secrets.listScopes()

# COMMAND ----------

clientID = dbutils.secrets.get("tokyo-olympic-scope", "tokyo-olympic-clientid")
secret = dbutils.secrets.get("tokyo-olympic-scope", "tokyo-olympsecret")
tenantID = dbutils.secrets.get("tokyo-olympic-scope", "tokyoolymp-directid")


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/tokyoolympic")

# COMMAND ----------

# clientID = dbutils.secrets.get("tokyo")
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{clientID}",
    "fs.azure.account.oauth2.client.secret": f"{secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"
}

dbutils.fs.mount(
    source = "abfss://tokyo-olympic-data@tokyoolympicdatawl.dfs.core.windows.net", #container@storageacc
    mount_point = "/mnt/tokyoolympic",
    extra_configs = configs
)
# dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic"

# COMMAND ----------


