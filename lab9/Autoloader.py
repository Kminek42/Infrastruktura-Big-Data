# Databricks notebook source
# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/FileStore/stream/events/"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
print(username)
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{table_name}", True)


# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/krzysiek_cegladanych_pl_etl_quickstart", True)


# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events

# COMMAND ----------

dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-4.json","/FileStore/stream/events/file-4.json",True)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/stream/events/"))

# COMMAND ----------

spark.read.format("json").load("dbfs:/FileStore/stream/events/").count()

# COMMAND ----------

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

df = spark.sql(f"select count(*) from {table_name}")
df.display()

# COMMAND ----------

display(dbutils.fs.ls("/tmp/krzysiek_cegladanych_pl/_checkpoint/etl_quickstart/sources/0/metadata/"))

# COMMAND ----------

# MAGIC %fs ls /tmp/krzysiek_cegladanych_pl/_checkpoint/etl_quickstart/metadata