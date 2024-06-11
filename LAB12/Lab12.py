# Databricks notebook source
# MAGIC %md
# MAGIC ### Zadanie 1
# MAGIC Compute->cluster->Driver logs, tam są okienka, Standart Output, Error Output i Log4j output. W Spark UI są informacje o jobs, stages, Storage, SQL/dataframes etc.
# MAGIC

# COMMAND ----------

# Zadanie 2
from pyspark.sql.types import *
df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/movies.csv")
display(df)

df.write.format("parquet").bucketBy(100, "year").saveAsTable("BucketedMovies4")
df.write.format("parquet").partitionBy("year").save("dbfs:/FileStore/tables/movies/moviesPartitioned.parquet")

display(dbutils.fs.ls("dbfs:/FileStore/tables"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/movies"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/movies/moviesPartitioned.parquet"))

# Przy użyciu partitionBy stworzył się plik dla każdego year. W przypadku bucketBy tabela jest zapisana w Hive i wykorzystuje hashowanie, plików powinno być tyle ile bucketów.

# COMMAND ----------

# Zadanie 3

display(spark.sql(f"ANALYZE TABLE bucketedMovies4 COMPUTE STATISTICS FOR ALL COLUMNS"))
display(spark.sql(f"DESCRIBE EXTENDED bucketedMovies4"))
display(spark.read.table("bucketedMovies4").describe())
