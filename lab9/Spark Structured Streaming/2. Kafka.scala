// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC <div>
// MAGIC   <h2> Ekosystem Kafki</h2>
// MAGIC
// MAGIC   <p>Oparty na wzorcu publikuj i zasubscrykuj</p>
// MAGIC
// MAGIC   <p>**publisher (nadawca)** tworzy wiadomość**message**</p>
// MAGIC
// MAGIC   <p>Wiadomość może zawierać klucz **key** potrzebny do partycjonowania</p>
// MAGIC
// MAGIC   <p>Wiadomości są wysyłade do **topic**</p>
// MAGIC
// MAGIC   <p>Topics są zarządzane przez **broker**</p>
// MAGIC
// MAGIC   <p>The broker jest punktem centralnym dla wszystkich wiadomości</p>
// MAGIC
// MAGIC   <p>Konsumer subskrybuje konkretny topic</p>
// MAGIC
// MAGIC   <p>Broker odpowiada za dostarczenie wiadomości do konsumenta</p>
// MAGIC </div>
// MAGIC <img style="float:right" src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/kafka.png"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Wymagania Kafki
// MAGIC
// MAGIC Kiedy pobierasz dane z Kafki musisz podać co najmniej dwie opcje:
// MAGIC
// MAGIC <p>1. Kafka bootstrap servers:</p>
// MAGIC <p>`option("kafka.bootstrap.servers", "server1.databricks.training:9092")`</p>
// MAGIC <p>2. Nazwę topica, do którego chcesz się zapisać.</p>

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Topic
// MAGIC
// MAGIC Są trzy metody sprecyzowanie topików:
// MAGIC
// MAGIC | Option        | Value                                          | Example |
// MAGIC | ------------- | ---------------------------------------------- | ------- |
// MAGIC | **subscribe** | Lista topików                | `dsr.option("subscribe", "topic1")` <br/> `dsr.option("subscribe", "topic1,topic2,topic3")` |
// MAGIC | **assign**    | JSON topics i partycje | `dsr.option("assign", "{'topic1': [1,3], 'topic2': [2,5]}")`
// MAGIC | **subscribePattern**   | (Java) regex           | `dsr.option("subscribePattern", "e[ns]")` <br/> `dsr.option("subscribePattern", "topic[123]")`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Kafka Server
// MAGIC
// MAGIC Dane pochodzą z edycji Wikipedii. 
// MAGIC
// MAGIC The Kafka server is fed by a separate TCP server that reads the Wikipedia edits, in real time, from the various language-specific IRC channels to which Wikimedia posts them. 
// MAGIC
// MAGIC * Kafka topic "en" odnosi się do en.wikipedia.org.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Dane
// MAGIC
// MAGIC `DataFrame` atrybuty:
// MAGIC * `key`           - klucz
// MAGIC * `value`         - format binarny Cast do STRING.
// MAGIC * `topic`         - topic
// MAGIC * `partition`     - partycja
// MAGIC * `offset`        - wartośc offset
// MAGIC * `timestamp`     - timestamp, potrzebny dla okna (windowing)
// MAGIC
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions._ 
spark.conf.set("spark.sql.shuffle.partitions", 8)

val editsDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")
  .option("subscribe", "en")
  .load()
  .select($"timestamp", $"value".cast("STRING").as("value"))

// COMMAND ----------

editsDF.printSchema

// COMMAND ----------

display(editsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### JSON
// MAGIC
// MAGIC W przypadku kolumny zawierającej zagnieżdżony json użyj `from_json()`
// MAGIC
// MAGIC * wymaga schematu

// COMMAND ----------

import java.net.{InetAddress, Socket}
import scala.io.Source

val addr = InetAddress.getByName("server1.databricks.training")
val socket = new Socket(addr, 9002)
val jsonString = Source.fromInputStream(socket.getInputStream).getLines.next
socket.close()

val mapper = new org.codehaus.jackson.map.ObjectMapper
val prettyJSON = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(
  mapper.readTree(jsonString)
)
println(prettyJSON)

// COMMAND ----------

// MAGIC %md
// MAGIC Można wczytać json string z danych bezpośrednio ze streamu, ręczny proces tworznenia schematu
// MAGIC
// MAGIC * tworzymy RDD ze stringa(`sc.parallelize`),
// MAGIC * tworzymy DataFrame z RDD,
// MAGIC * wyciągamy schemat
// MAGIC * println

// COMMAND ----------

import org.apache.spark.sql.types._

// Funkcja formatująca jsona
def prettySchema(schema: StructType, indentation: Int = 2): String = {
  def prettyStruct(st: StructType, indentationLevel: Int): String = {
    val indentSpaces = " " * (indentationLevel * indentation)
    val prefix = s"${indentSpaces}StructType(List(\n"
    val fieldIndentSpaces = " " * ((indentationLevel + 1) * indentation)
    val fieldStrings: Seq[String] = for (field <- st.fields) yield {
      val fieldPrefix = s"""${fieldIndentSpaces}StructField("${field.name}", """

      val fieldType = field.dataType match {
        case st2: StructType =>  s"${prettyStruct(st2, indentationLevel + 1)}"
        case _               =>  s"${field.dataType}"
      }
      
      s"$fieldPrefix$fieldType, ${field.nullable})"
    }
    val fields = fieldStrings.mkString(",\n")
    s"$prefix$fields\n$indentSpaces))"
  }
  
  prettyStruct(schema, 0)
}

val inferredSchema = spark.read.json(sc.parallelize(Array(jsonString))).schema
println(prettySchema(inferredSchema))

// COMMAND ----------

// MAGIC %md
// MAGIC W tym wypadku jest błąd w schemacie lat i long są złym typem danych, trzeba to ręcznie naprawić

// COMMAND ----------

val schema = StructType(List(
  StructField("channel", StringType, true),
  StructField("comment", StringType, true),
  StructField("delta", IntegerType, true),
  StructField("flag", StringType, true),
  StructField("geocoding", StructType(List(
    StructField("city", StringType, true),
    StructField("country", StringType, true),
    StructField("countryCode2", StringType, true),
    StructField("countryCode3", StringType, true),
    StructField("stateProvince", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true)
  )), true),
  StructField("isAnonymous", BooleanType, true),
  StructField("isNewPage", BooleanType, true),
  StructField("isRobot", BooleanType, true),
  StructField("isUnpatrolled", BooleanType, true),
  StructField("namespace", StringType, true),
  StructField("page", StringType, true),
  StructField("pageURL", StringType, true),
  StructField("timestamp", StringType, true),
  StructField("url", StringType, true),
  StructField("user", StringType, true),
  StructField("userURL", StringType, true),
  StructField("wikipediaURL", StringType, true),
  StructField("wikipedia", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Trzeba sparsować json

// COMMAND ----------

val jsonEdits = editsDF.select($"timestamp", from_json($"value", schema).as("json"))

// COMMAND ----------

jsonEdits.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Wypłaszczamy json
// MAGIC

// COMMAND ----------

val editsFinalDF = jsonEdits
  .select($"json.wikipedia".as("wikipedia"),
          $"json.isAnonymous".as("isAnonymous"),
          $"json.namespace".as("namespace"),
          $"json.page".as("page"),
          $"json.pageURL".as("pageURL"),
          $"json.geocoding".as("geocoding"),
          unix_timestamp($"json.timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").as("timestamp"),
          $"json.user".as("user"))

// COMMAND ----------

editsFinalDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Anonimowe edycje
// MAGIC
// MAGIC Jeśli w danych nie ma nazwy loginu tylko adres IP to znaczy że edycja jest anonimowa.
// MAGIC
// MAGIC Rozpoczynamy stream
// MAGIC
// MAGIC `editsFinalDF.writeStream.format("...").start()`
// MAGIC
// MAGIC Wygodna do analizy jest tabla in memory
// MAGIC

// COMMAND ----------

val anonDF = editsFinalDF
  .filter($"namespace" === "article")
  .filter($"isAnonymous" === true)
  .filter(! isnull($"geocoding.countryCode3"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Wizualizacja

// COMMAND ----------

display(
  anonDF
    .groupBy( window($"timestamp", "1 second", "500 milliseconds"), 
              $"geocoding.countryCode3")
    .count()
    .select($"countryCode3", $"count")
)

// COMMAND ----------

import org.apache.spark.sql.streaming.StreamingQuery
spark.streams.active.foreach((s: StreamingQuery) => s.stop())