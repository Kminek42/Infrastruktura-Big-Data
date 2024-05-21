// Databricks notebook source
// MAGIC %md ## Dane
// MAGIC Dane są dostępne na AWS i dostęp zapewnia Databricks `/databricks-datasets/structured-streaming/events/` 

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/structured-streaming/events/

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

// COMMAND ----------

// MAGIC %md 
// MAGIC * Stwórz osobny folder 'streamDir' i przekopuj 40 plików. możesz użyć dbutils....
// MAGIC * Pozostałe pliki będziesz kopiować jak stream będzie aktywny

// COMMAND ----------

// MAGIC %python
// MAGIC import os
// MAGIC target_dir = "/streamDir"
// MAGIC source_dir = "/databricks-datasets/structured-streaming/events/"
// MAGIC
// MAGIC dbutils.fs.mkdirs(target_dir)
// MAGIC
// MAGIC files = dbutils.fs.ls(source_dir)
// MAGIC
// MAGIC selected_files = files[:40]
// MAGIC
// MAGIC for file in selected_files:
// MAGIC     file_name = os.path.basename(file.path)
// MAGIC     dbutils.fs.cp(file.path, target_dir + "/" + file_name)

// COMMAND ----------

// MAGIC %md ## Analiza danych
// MAGIC * Stwórz schemat danych i wyświetl zawartość danych z oginalnego folderu

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC
// MAGIC inputPath = "/databricks-datasets/structured-streaming/events/"
// MAGIC jsonSchema = StructType([
// MAGIC     StructField("time", TimestampType(), nullable=False),
// MAGIC     StructField("action", StringType(), nullable=False)
// MAGIC ])
// MAGIC df = spark.read.json(inputPath, schema=jsonSchema)
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md 
// MAGIC Policz ilość akcji "open" i "close" w okienku (window) jedno godzinnym (kompletny folder). 

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC
// MAGIC okno = window(df["time"], "1 hour")
// MAGIC iloscAkcji= df.groupBy(okno, df["action"]).count().alias("count")
// MAGIC
// MAGIC iloscAkcji.createOrReplaceTempView("static_counts")
// MAGIC
// MAGIC display(iloscAkcji)

// COMMAND ----------

// MAGIC %md 
// MAGIC Użyj sql i pokaż na wykresie ile było akcji 'open' a ile 'close'.

// COMMAND ----------

// MAGIC %sql select action, sum(count) as total_count from static_counts group by action

// COMMAND ----------

// MAGIC %md
// MAGIC Użyj sql i pokaż ile było akcji w każdym dniu i godzinie przykład ('Jul-26 09:00')

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from static_counts order by time, action

// COMMAND ----------

// MAGIC %md ## Stream Processing 
// MAGIC Teraz użyj streamu.
// MAGIC * Ponieważ będziesz streamować pliki trzeba zasymulować, że jest to normaly stream. Podpowiedź dodaj opcję 'maxFilesPerTrigger'
// MAGIC * Użyj 'streamDir' niekompletne pliki

// COMMAND ----------

// MAGIC %python
// MAGIC # odpal stream
// MAGIC streamingInputDF = spark.readStream.format("json").schema(jsonSchema).option("maxFilesPerTrigger", 1).load('dbfs:/streamDir')
// MAGIC
// MAGIC # sumujemy open i close tak ja jak powyżej w okienku jednogodzinnym
// MAGIC okno = window(streamingInputDF["time"], "1 hour")
// MAGIC streamingCountsDF = streamingInputDF.groupBy(okno, streamingInputDF["action"]).count().alias("count")
// MAGIC display(streamingCountsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Sprawdź czy stream działa

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC streamingInputDF.isStreaming

// COMMAND ----------

// MAGIC %md 
// MAGIC * Zredukuj partyce shuffle do 4 
// MAGIC * Teraz ustaw Sink i uruchom stream
// MAGIC * użyj formatu 'memory'
// MAGIC * 'outputMode' 'complete'

// COMMAND ----------

// MAGIC %python
// MAGIC stream = spark.readStream.format("json").schema(jsonSchema).option("maxFilesPerTrigger", 1).load("dbfs:/streamDir").repartition(4)
// MAGIC okno = window(stream["time"], "1 hour")
// MAGIC stream_zliczenia = stream.groupBy(okno, stream["action"]).count().alias("count")
// MAGIC query = stream_zliczenia.writeStream.format("memory").outputMode("complete").queryName("counts").start()

// COMMAND ----------

// MAGIC %md 
// MAGIC `query` działa teraz w tle i wczytuje pliki cały czas uaktualnia count. Postęp widać w Dashboard

// COMMAND ----------

Thread.sleep(3000) // lekkie opóźnienie żeby poczekać na wczytanie plików

// COMMAND ----------

// MAGIC %md
// MAGIC * Użyj sql żeby pokazać ilość akcji w danym dniu i godzinie 

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

// COMMAND ----------

// MAGIC %md 
// MAGIC * Sumy mogą się nie zgadzać ponieważ wcześniej użyłeś niekompletnych danych.
// MAGIC * Teraz przekopiuj resztę plików z orginalnego folderu do 'streamDir', sprawdź czy widać zmiany 
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC target_dir = "/streamDir"
// MAGIC source_dir = "/databricks-datasets/structured-streaming/events/"
// MAGIC
// MAGIC files = dbutils.fs.ls(source_dir)
// MAGIC
// MAGIC for file in files:
// MAGIC     file_name = os.path.basename(file.path)
// MAGIC     dbutils.fs.cp(file.path, target_dir + "/" + file_name)

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- użyj zapytania jak wcześniej pokazujący symy z datą i godziną powinny pasować do danych z pierwszego statycznego DF
// MAGIC select action, date_format(window.end, "MMM-dd HH:mm") as time, count from result order by time, action

// COMMAND ----------

// MAGIC %md
// MAGIC * Zatrzymaj stream

// COMMAND ----------

// MAGIC %python
// MAGIC query.stop()