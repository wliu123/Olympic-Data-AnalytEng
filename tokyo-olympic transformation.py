# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DateType

# COMMAND ----------

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

athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolympic/raw-data/teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
    .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold = medals.orderBy("Gold", ascending = False).select("Team_Country", "Gold").show()

# COMMAND ----------

#Calculate the fraction number of entries by gender for each discipline
average_entries = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transformed-data/athletes")

# COMMAND ----------

coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transformed-data/teams")

# COMMAND ----------


