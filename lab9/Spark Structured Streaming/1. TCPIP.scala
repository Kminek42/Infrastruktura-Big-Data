// Databricks notebook source
// MAGIC %md
// MAGIC ## Przykłady TCP/IP
// MAGIC
// MAGIC * Streaming DataFrame (SDF).
// MAGIC * SDF z TCP/IP socket.
// MAGIC * Jak pracować z SDF.
// MAGIC * Specjalne rozważania SDF.
// MAGIC * Windowing.
// MAGIC * Watermarking.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tworzenie Streaming DataFrame
// MAGIC
// MAGIC Jak wszystko w sparku rozpoczyna się od `SparkSession`. W notatnikach Databricks jest już dostępny `spark`.
// MAGIC
// MAGIC ### Format `socket` 
// MAGIC Ten typ powinien być użyty tylko do testowania ponieważ nie ma tolerancji na błędy!!!

// COMMAND ----------

val logsDF = spark.readStream
  .format("socket")
  .option("host", "server1.databricks.training")
  .option("port", 9091)
  .load()

// COMMAND ----------

val file_path = "/databricks-datasets/structured-streaming/events"
val checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

val raw_df = spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(file_path)


// COMMAND ----------

display(raw_df)

// COMMAND ----------

// MAGIC %md
// MAGIC W tym przykładzie każda linia ze streamu jest wierszem w DataFrame w kolumnie `value`.

// COMMAND ----------

logsDF.printSchema()

// COMMAND ----------

display(logsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Wizualizacja streamu
// MAGIC Display daje możliwość zmiany formy do wyświetlenia np (bar lub graph), który będzie się uaktualniał automatycznie.

// COMMAND ----------

import org.apache.spark.sql.functions._

val runningCountsDF = logsDF.agg(count("*"))

display(runningCountsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Stream DataFrame
// MAGIC
// MAGIC Na DataFrame pochodzącej z przysyłu strumieniowego możesz wykonać takie same transformacje jak na normalym statycznym DataFrame. 
// MAGIC
// MAGIC Przykładowa operacja poniżej
// MAGIC
// MAGIC Użycie regex czasami wymaga pomocy https://regex101.com/

// COMMAND ----------


val cleanDF = logsDF
  .withColumn("ts_string", $"value".substr(2, 23))
  .withColumn("epoch", unix_timestamp($"ts_string", "yyyy/MM/dd HH:mm:ss.SSS"))
  .withColumn("capturedAt", $"epoch".cast("timestamp"))
  .withColumn("logData", regexp_extract($"value", """^.*\]\s+(.*)$""", 1))
  
cleanDF.printSchema()


// COMMAND ----------

// Filtrujemy wszystkie błędy logów
val errorsDF = cleanDF
  .select($"capturedAt", $"logData")
  .filter( $"value" like "% (ERROR) %" )

errorsDF.printSchema()

// COMMAND ----------

display(errorsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Spróbuj posortować DataFrame

// COMMAND ----------

display(
  errorsDF.orderBy($"capturedAt".desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## AnalysisException
// MAGIC Nie wszystkie operacje są dozwolone. Na przykładzie powyżej Spark musiałby przesortować dane a tym samym wykonać shuffle, co spowodowało by utratę stabilości i dlatego jest niedozwolone. 
// MAGIC  <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">Unsupported Operations</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Windowing
// MAGIC
// MAGIC Jak odpowiedzieć na pytanie "Ile jest rekordów co sekundę?"
// MAGIC
// MAGIC Przykładowa agregacja na okienku sliding window.
// MAGIC [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time):
// MAGIC
// MAGIC >In a grouped aggregation, aggregate values (e.g., counts) are maintained for each unique value in the user-specified grouping column. In the case of _window_-based
// MAGIC > aggregations, aggregate values are maintained for each window the event-time of a row falls into.
// MAGIC
// MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Co to jest "event time"?
// MAGIC
// MAGIC _Event time_ jest to informacja kiedy dane zostały wygenerowane, nie jest to zarządzane przez Structured Streaming. Przykład to timestamp z logów kiedy system zapisał jakieś zdażenie. 
// MAGIC
// MAGIC Structured Streaming dostarcza funkcjonalość, która pozwala pracować z danymi posiadającymi czas zdażenia. Developer musi powiedzieć która kolumna jest typem event time (to musi być `Timestamp`).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Przykład funkcji window.
// MAGIC
// MAGIC Pamiętaj jest kilka wersji funkcji `window()` tumbling, sliding, session !!! 
// MAGIC
// MAGIC * Początek okna jest włączny (inclusive)) a konieć wyłączny (exclusive)
// MAGIC * przykład 12:05 będzie w oknie \[12:05,12:10) ale nie w \[12:00,12:05).
// MAGIC * Funkcja window wspiera precycje do microsekundy.
// MAGIC

// COMMAND ----------

val messagesPerSecond = cleanDF
  .select($"capturedAt")
  .groupBy( window($"capturedAt", "1 second") )
  .count()
  .orderBy("window.start")

messagesPerSecond.printSchema()

// COMMAND ----------

display(
  messagesPerSecond
    .select($"window.start".as("start"), 
            $"window.end".as("end"), 
            $"count" )
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### UWAGA NA SHUFFLE PARTITIONS
// MAGIC
// MAGIC Sprawdź **Spark Jobs** ile jest partycji bo może się okazać, że masz wartość default = 200. Będzie to miało wpływ na osiągi.
// MAGIC
// MAGIC W przypadku strumienia strukturalnego Spark utrzymuje agregacje w przedziale czasowym. Jest to obiekt w pamięci _state map_ dla każdego okna w każdej z partycji. Po przetwożeniu każdego okna 'state maps' zostaje zapisana na dysku co daje odporność na błędy. 
// MAGIC
// MAGIC Żeby zmiejszyć impact tego procesu zmień ilośc partycji 'shuffle'.

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

// COMMAND ----------

display(
  messagesPerSecond.select($"window.start".as("start"), 
                           $"window.end".as("end"), 
                           $"count" )
)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC ## Dodatkowe problemy z osiągami
// MAGIC
// MAGIC Oprócz opóźnien związanych z shuffle jest drugi problem z ilością okienek. W naszym przykładzie jedno okno jest generowane co sekundę, i każde z nich musi być utrzymane i z czasem będzie opóźniało proces. 
// MAGIC
// MAGIC Żeby to usprawnić można zwiększyć wielkośc okna do 1 minuty co zmiejszy ilość okien.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Watermarking
// MAGIC
// MAGIC Najlepszą metodą jest usunięcie starych okienek przy użyciu watermark.  

// COMMAND ----------

// MAGIC %md
// MAGIC ### Przykład
// MAGIC
// MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-late-data.png"/>
// MAGIC
// MAGIC Spark musi wiedzieć kiedy usunąć stare agregacje żeby oczyścić pamięć.
// MAGIC
// MAGIC Wystarczy podać kolumnę z event time i ile czasu dane mogą być spóźnione. 
// MAGIC

// COMMAND ----------


val windowedDF = errorsDF
  .withWatermark("capturedAt", "1 minute")
  .groupBy( window($"capturedAt", "1 second") )
  .count()
  .select( $"window.start".as("start"), 
           $"window.end".as("end"), 
           $"count" )
  .orderBy($"start")

display(windowedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Checkpointing
// MAGIC
// MAGIC Możesz odzyskać poprzednie agregacje i stan streamu z poprzedniego uruchomienia. 
// MAGIC Wymagana jest ścieżka w HDFS. 
// MAGIC
// MAGIC Proces zapisze 
// MAGIC * zakres przesunięć przetwarzanych w każdym trigerze
// MAGIC * wartości agregacji
// MAGIC

// COMMAND ----------

// czyścimy przed użyciem
val checkpointDir = "/mnt/data/checkpoint"
// dbutils.fs.rm(checkpointDir, true)


// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val streamingQuery = windowedDF
  .writeStream
  .outputMode("complete")                        // tryb zapisu
  .option("checkpointLocation", checkpointDir)   // Checkpointing
  .format("memory")                              // sink
  .queryName("my_query")                         // nazwa zapytania
  .trigger(Trigger.ProcessingTime("5 seconds"))  // przedział wyzwalacza 
  .start()                                       // uruchomienie

// COMMAND ----------

//sprawdź co się dzieje w lokalizacji checkpointa
display(dbutils.fs.ls(checkpointDir))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Batch Mode
// MAGIC
// MAGIC Zapytanie w poprzednim procesie zapisuje do  pamięci, możesz użyć SQL do pobrania danch z tabeli 

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from my_query

// COMMAND ----------

display(
  spark.read.table("my_query")
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Obiekt StreamingQuery
// MAGIC
// MAGIC Masz możliwość sprawdzenia tego co dzieje się w streamie. `streamingQuery` zewraca `DataStreamWriter.start()` 

// COMMAND ----------

println(streamingQuery.name)

// COMMAND ----------

println(streamingQuery.status)

// COMMAND ----------

println(streamingQuery.lastProgress)

// COMMAND ----------

println(streamingQuery.explain())

// COMMAND ----------

streamingQuery.stop()

// COMMAND ----------

import org.apache.spark.sql.streaming.StreamingQuery
spark.streams.active.foreach((s: StreamingQuery) => s.stop())