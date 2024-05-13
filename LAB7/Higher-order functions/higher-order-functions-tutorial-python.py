# Databricks notebook source
# MAGIC %md # Higher-Order and Lambda Functions: Explore Complex and Structured Data in SQL 

# COMMAND ----------

# MAGIC %md This tutorial walks you through four higher-order functions. While this in-depth [blog](https://databricks.com/blog/2017/05/24/working-with-nested-data-using-higher-order-functions-in-sql-on-databricks.html) explains the concepts, justifications, and motivations of _why_ handling complex data types such as arrays are important in SQL, and equally explains why their existing implementation are inefficient and cumbersome, this tutorial shows _how_ to use higher-order functions in SQL in processing structured data and arrays in IoT device events. In particular, they come handy and you can put them to good use if you enjoy functional programming and can quickly and can efficiently write a lambda expression as part of these higher-order SQL functions. 
# MAGIC
# MAGIC This tutorial explores four functions and how you can put them to a wide range of uses in your processing and transforming array types:
# MAGIC
# MAGIC * `transform()`
# MAGIC * `filter()`
# MAGIC * `exists()`
# MAGIC * `aggregate()`
# MAGIC
# MAGIC The takeaway from this short tutorial is that there exists myriad ways to slice and dice nested JSON structures with Spark SQL utility functions. These dedicated higher-order functions are primarily suited to manipulating arrays in Spark SQL, making it easier and the code more concise when processing table values with arrays or nested arrays. 

# COMMAND ----------

# MAGIC %md Let's create a simple JSON schema with attributes and values, with at least two attributes that are arrays, namely _temp_ and _c02_level_

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType() \
          .add("dc_id", StringType()) \
          .add("source", MapType(StringType(), StructType() \
                        .add("description", StringType()) \
                        .add("ip", StringType()) \
                        .add("id", IntegerType()) \
                        .add("temp", ArrayType(IntegerType())) \
                        .add("c02_level", ArrayType(IntegerType())) \
                        .add("geo", StructType() \
                              .add("lat", DoubleType()) \
                              .add("long", DoubleType()))))


# COMMAND ----------

# MAGIC %md This helper Python function converts a JSON string into a [Python DataFrame](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html#pyspark-sql-dataframe).

# COMMAND ----------

# Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

# COMMAND ----------

# MAGIC %md Using the schema above, create a complex JSON stucture and create into a Python DataFrame. Display the DataFrame gives us two columns: a key (dc_id) and value (source), which is JSON string with embedded nested structure.

# COMMAND ----------

dataDF = jsonToDataFrame( """{

    "dc_id": "dc-101",
    "source": {
        "sensor-igauge": {
        "id": 10,
        "ip": "68.28.91.22",
        "description": "Sensor attached to the container ceilings",
        "temp":[35,35,35,36,35,35,32,35,30,35,32,35],
        "c02_level": [1475,1476,1473],
        "geo": {"lat":38.00, "long":97.00}                        
      },
      "sensor-ipad": {
        "id": 13,
        "ip": "67.185.72.1",
        "description": "Sensor ipad attached to carbon cylinders",
        "temp": [45,45,45,46,45,45,42,35,40,45,42,45],
        "c02_level": [1370,1371,1372],
        "geo": {"lat":47.41, "long":-122.00}
      },
      "sensor-inest": {
        "id": 8,
        "ip": "208.109.163.218",
        "description": "Sensor attached to the factory ceilings",
        "temp": [40,40,40,40,40,43,42,40,40,45,42,45],
        "c02_level": [1346,1345, 1343],
        "geo": {"lat":33.61, "long":-111.89}
      },
      "sensor-istick": {
        "id": 5,
        "ip": "204.116.105.67",
        "description": "Sensor embedded in exhaust pipes in the ceilings",
        "temp":[30,30,30,30,40,43,42,40,40,35,42,35],
        "c02_level": [1574,1570, 1576],
        "geo": {"lat":35.93, "long":-85.46}
      }
    }
  }""", schema)

display(dataDF)
  

# COMMAND ----------

# MAGIC %md By examining its schema, you can notice that the DataFrame schema reflects the above defined schema, where two of its elments are are arrays of integers. 

# COMMAND ----------

dataDF.printSchema()

# COMMAND ----------

# MAGIC %md Employ `explode()` to explode the column `source` into its individual columns.

# COMMAND ----------

explodedDF = dataDF.select("dc_id", explode("source"))
display(explodedDF)

# COMMAND ----------

# MAGIC %md Now you can work with the `value` column, which a is `struct`, to extract individual fields by using their names.

# COMMAND ----------

#
# use col.getItem(key) to get individual values within our Map
#
devicesDataDF = explodedDF.select("dc_id", "key", \
                        "value.ip", \
                        col("value.id").alias("device_id"), \
                        col("value.c02_level").alias("c02_levels"), \
                        "value.temp")
display(devicesDataDF)

# COMMAND ----------

# MAGIC %md For sanity let's ensure what was created as DataFrame was preserved and adherent to the schema declared above while exploding and extracting individual data items.

# COMMAND ----------

devicesDataDF.printSchema()

# COMMAND ----------

# MAGIC %md Now, since this tutorial is less about DataFrames API and more about higher-order functions and lambdas in SQL, create a temporary table or view and start using the higher-order SQL functions mentioned above.

# COMMAND ----------

devicesDataDF.createOrReplaceTempView("data_center_iot_devices")

# COMMAND ----------

# MAGIC %md The table was created as columns in your DataFrames and it reflects its schema.

# COMMAND ----------

# MAGIC %sql select * from data_center_iot_devices

# COMMAND ----------

# MAGIC %sql describe data_center_iot_devices

# COMMAND ----------

# MAGIC %md ## SQL Higher-Order Functions and Lambda Expressions

# COMMAND ----------

# MAGIC %md ### How to use `transform()`

# COMMAND ----------

# MAGIC %md Its functional signature, `transform(values, value -> lambda expression)`, has two components:
# MAGIC
# MAGIC 1. `transform(values..)` is the higher-order function. This takes an array and an anonymous function as its input. Internally, `transform` takes care of setting up a new array, applying the anonymous function to each element, and then assigning the result to the output array.
# MAGIC 2.  The `value -> expression`  is an anonymous function. The function is further divided into two components separated by a `->` symbol:
# MAGIC   * **The argument list**: This case has only one argument: value. You can specify multiple arguments by creating a comma-separated list of arguments enclosed by parenthesis, for example: `(x, y) -> x + y.`
# MAGIC   * **The body**: This is a SQL expression that can use the arguments and outer variables to calculate the new value. 
# MAGIC   
# MAGIC In short, the programmatic signature for `transform()` is as follows:
# MAGIC
# MAGIC `transform(array<T>, function<T, U>): array<U>`
# MAGIC This produces an array by applying a function<T, U> to each element of an input array<T>.
# MAGIC Note that the functional programming equivalent operation is `map`. This has been named transform in order to prevent confusion with the map expression (that creates a map from a key value expression).
# MAGIC
# MAGIC This basic scheme for `transform(...)` works the same way as with other higher-order functions, as you will see shortly.

# COMMAND ----------

# MAGIC %md 
# MAGIC The following query transforms the values in an array by converting each elmement's temperature reading from Celsius to Fahrenheit.
# MAGIC
# MAGIC Let's transform (and hence convert) all our Celsius reading into Fahrenheit. (Use conversion formula: `((C * 9) / 5) + 32`) The lambda expression here is the formula to convert **C->F**.
# MAGIC Now, `temp` and `((t * 9) div 5) + 32` are the arguments to the higher-order function `transform()`. The anonymous function will iterate through each element in the array, `temp`, apply the function to it, and transforming its value and placing into an output array. The result is a new column with tranformed values: `fahrenheit_temp`.

# COMMAND ----------

# MAGIC %sql select key, ip, device_id, temp,
# MAGIC      transform (temp, t -> ((t * 9) div 5) + 32 ) as fahrenheit_temp
# MAGIC      from data_center_iot_devices

# COMMAND ----------

# MAGIC %md While the above example generates transformed values, this example uses a Boolean expression as a lambda function and generates a boolean array of results instead of values, since the expression 
# MAGIC `t->t > 1300` is a predicate, resulting into a true or false.

# COMMAND ----------

# MAGIC %sql select dc_id, key, ip, device_id, c02_levels, temp, 
# MAGIC      transform (c02_levels, t -> t > 1300) as high_c02_levels
# MAGIC      from data_center_iot_devices
# MAGIC     

# COMMAND ----------

# MAGIC %md ### How to use `filter()`

# COMMAND ----------

# MAGIC %md As with `transform`, `filter` has a similar signature, `filter(array<T>, function<T, Boolean>): array<T>`
# MAGIC Unlike `transform()` with a boolean expression, it produces an output array from an input array by *only* adding elements for which predicate `function<T, Boolean>` holds.
# MAGIC
# MAGIC For instance, let's include only readings in our `c02_levels` that exceed dangerous levels (`cO2_level > 1300`). Again the functional signature is not dissimilar to `transform()`. However, note the difference in how `filter()` generated the resulting array compared to _transform()_ with similar lambda expression.

# COMMAND ----------

# MAGIC %sql select dc_id, key, ip, device_id, c02_levels, temp, 
# MAGIC      filter (c02_levels, t -> t > 1300) as high_c02_levels
# MAGIC      from data_center_iot_devices

# COMMAND ----------

# MAGIC %md Notice when the lambda's predicate expression is reversed, the resulting array is empty. That is, it does not evaluate to values true or false as it did in `tranform()`.

# COMMAND ----------

# MAGIC %sql select dc_id, key, ip, device_id, c02_levels, temp, 
# MAGIC      filter (c02_levels, t -> t < 1300 ) as high_c02_levels
# MAGIC      from data_center_iot_devices

# COMMAND ----------

# MAGIC %md ### How to use `exists()`

# COMMAND ----------

# MAGIC %md A mildly different functional signature than the above two functions, the idea is simple and same: 
# MAGIC
# MAGIC `exists(array<T>, function<T, V, Boolean>): Boolean`
# MAGIC Return true if predicate `function<T, Boolean>` holds for any element in input array.
# MAGIC
# MAGIC In this case you can iterate through the `temp` array and see if a particular value exists in the array. Let's acertain if any of your values contains 45 degrees Celsius or determine of a c02 level in any of the readings equals to 1570.
# MAGIC

# COMMAND ----------

# MAGIC %sql select dc_id, key, ip, device_id, c02_levels, temp, 
# MAGIC      exists (temp, t -> t = 45 ) as value_exists
# MAGIC      from data_center_iot_devices

# COMMAND ----------

# MAGIC %sql select dc_id, key, ip, device_id, c02_levels, temp, 
# MAGIC      exists (c02_levels, t -> t = 1570 ) as high_c02_levels
# MAGIC      from data_center_iot_devices

# COMMAND ----------

# MAGIC %md ### How to use `reduce()`
# MAGIC

# COMMAND ----------

# MAGIC %md By far this function and its method is more advanced than others. It also allows you to do aggregation, as seen in the next section.
# MAGIC Its signature allows us to some extra bit with the last lambda expression as its functional argument.
# MAGIC
# MAGIC `reduce(array<T>, B, function<B, T, B>, function<B, R>): R`
# MAGIC Reduce the elements of `array<T>` into a single value `R` by merging the elements into a buffer B using `function<B, T, B>` and by applying a finish `function<B, R>` on the final buffer. The initial value `B` is determined by a `zero` expression. 
# MAGIC
# MAGIC The finalize function is optional, if you do not specify the function the finalize function the identity function (`id -> id`) is used.
# MAGIC This is the only higher-order function that takes two lambda functions.
# MAGIC
# MAGIC For instance, if you want to compute an average of the temperature readings, use lambda expressions: The first one accumulates all the results into an internal temporary buffer, and the second function applies to the final accumulated buffer. With respect to our signature above, `B` is `0`; `function<B,T,B>` is `t + acc`, and `function<B,R>` is `acc div size(temp)`. Furthermore, in the finalize lambda expression, convert the average temperature to Fahrenheit.

# COMMAND ----------

# MAGIC %sql select key, ip, device_id, temp,
# MAGIC     reduce(temp, 0, (t, acc) -> t + acc, acc-> (acc div size(temp) * 9 div 5) + 32 ) as average_f_temp
# MAGIC     from data_center_iot_devices
# MAGIC     sort by average_f_temp desc

# COMMAND ----------

# MAGIC %md Simillarly, `reduce()` is employed here to get an average of c02_levels.

# COMMAND ----------

# MAGIC %sql select key, ip, device_id, c02_levels,
# MAGIC     reduce(c02_levels, 0, (t, acc) -> t + acc, acc-> acc div size(c02_levels)) as average_c02_levels
# MAGIC     from data_center_iot_devices
# MAGIC     sort by  average_c02_levels desc
# MAGIC

# COMMAND ----------

# MAGIC %md ### How to use `aggregate()`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregate is an alias of `reduce`. It has the same inputs, and it will produce the same results.
# MAGIC   
# MAGIC Let's compute a geomean of the c02 levels and sort them by descending order. Note the complex lambda expression with the above functional signature.

# COMMAND ----------

# MAGIC %sql select key, ip, device_id, c02_levels,
# MAGIC      aggregate(c02_levels,
# MAGIC                (1.0 as product, 0 as N),
# MAGIC                (buffer, c02) -> (c02 * buffer.product, buffer.N+1),
# MAGIC                buffer -> round(Power(buffer.product, 1.0 / buffer.N))) as c02_geomean
# MAGIC      from data_center_iot_devices
# MAGIC      sort by c02_geomean desc

# COMMAND ----------

# MAGIC %md ## Another example using similar nested structure with IoT JSON data.
# MAGIC
# MAGIC Let's create a DataFrame based on this schema and check if all is good.

# COMMAND ----------

schema2 = StructType() \
                    .add("device_id", IntegerType()) \
                    .add("battery_level", ArrayType(IntegerType())) \
                    .add("c02_level", ArrayType(IntegerType())) \
                    .add("signal", ArrayType(IntegerType())) \
                    .add("temp", ArrayType(IntegerType())) \
                    .add("cca3", ArrayType(StringType())) \
                    .add("device_type", StringType()) \
                    .add("ip", StringType()) \
                    .add("timestamp", TimestampType())

# COMMAND ----------

dataDF2 = jsonToDataFrame("""[
  {"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": ["USA", "United States"], "temp": [25,26, 27], "signal": [23,22,24], "battery_level": [8,9,7], "c02_level": [917, 921, 925], "timestamp" :1475600496 }, 
  {"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": ["NOR", "Norway"], "temp": [30, 32,35], "signal": [18,18,19], "battery_level": [6, 6, 5], "c02_level": [1413, 1416, 1417], "timestamp" :1475600498 }, 
  {"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": ["USA", "United States"], "temp":[47, 47, 48], "signal": [12,12,13], "battery_level": [1, 1, 0],  "c02_level": [1447,1446, 1448], "timestamp" :1475600502 }, 
  {"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3":["PHL", "Philippines"], "temp":[29, 29, 28], "signal":[11, 11, 11], "battery_level":[0, 0, 0], "c02_level": [983, 990, 982], "timestamp" :1475600504 },
  {"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": ["USA", "United States"], "temp":[50,51,50], "signal": [16,16,17], "battery_level": [8,8, 8], "c02_level": [1574,1575,1576], "timestamp" :1475600506 }, 
  {"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": ["CHN", "China"], "temp": [21,21,22], "signal": [18,18,19], "battery_level": [9,9,9], "c02_level": [1249,1249,1250], "timestamp" :1475600508 },
  {"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": ["JPN", "Japan"], "temp":[27,27,28], "signal": [15,15,29], "battery_level":[0,0,0], "c02_level": [1531,1532,1531], "timestamp" :1475600512 },
  {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": ["USA", "United States"], "temp":[40,40,41], "signal": [16,16,17], "battery_level":[ 9, 9, 10], "c02_level": [1208,1209,1208], "timestamp" :1475600514},
  {"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": ["ITA", "Italy"], "temp": [19,28,5], "signal": [11, 5, 24], "battery_level": [0,-1,0], "c02_level": [1171, 1240, 1400], "timestamp" :1475600516 },
  {"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": ["USA", "United States"], "temp": [32,33,32], "signal": [26,26,25], "battery_level": [7,7,8], "c02_level": [886,886,887], "timestamp" :1475600518 },
  {"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": ["IND", "India"], "temp": [46,45,44], "signal": [25,25,24], "battery_level": [4,5,5], "c02_level": [863,862,864], "timestamp" :1475600520 },
  {"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": ["NOR", "Norway"], "temp": [18,17,18], "signal": [26,25,26], "battery_level": [8,9,8], "c02_level": [1220,1221,1220], "timestamp" :1475600522 },
  {"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": ["USA", "United States"], "temp": [34,35,34], "signal": [20,21,20], "battery_level": [8,8,8], "c02_level": [1504,1504,1503], "timestamp" :1475600524 },
  {"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": ["USA", "United States"], "temp": [39,40,38], "signal": [17, 17, 18], "battery_level": [8,8,7], "c02_level": [831,832,831], "timestamp" :1475600526 },
  {"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": ["USA", "United States"], "temp": [27,27,28], "signal": [26,26,25], "battery_level": [5,5,5], "c02_level": [1378,1376,1378], "timestamp" :1475600528 },
  {"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": ["CHN", "China"], "temp": [10,10,11], "signal": [24,24,23], "battery_level": [6,5,6], "c02_level": [1423, 1423, 1423], "timestamp" :1475600530 },
  {"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": ["USA", "United States"], "temp": [38,38,39], "signal": [17,17,17], "battery_level": [9,9,9], "c02_level": [1304,1304,1304], "timestamp" :1475600532 },
  {"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": ["USA", "United States"], "temp": [26, 0, 99], "signal": [10, 1, 5], "battery_level": [0, 0, 0], "c02_level": [902,902, 1300], "timestamp" :1475600534 },
  {"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": ["AUT", "Austria"], "temp": [32,32,33], "signal": [27,27,28], "battery_level": [5,5,5], "c02_level": [1282, 1282, 1281], "timestamp" :1475600536 }
  ]""", schema2)

display(dataDF2)
 

# COMMAND ----------

dataDF2.printSchema()

# COMMAND ----------

# MAGIC %md As above, let's create a temporary view to which you can issue SQL queries and do some processing using higher-order functions.

# COMMAND ----------

dataDF2.createOrReplaceTempView("iot_nested_data")

# COMMAND ----------

# MAGIC %md ### How to use `transform()`

# COMMAND ----------

# MAGIC %md Use transform to check battery level.

# COMMAND ----------

# MAGIC %sql select cca3, device_type, battery_level,
# MAGIC      transform (battery_level, bl -> bl > 0) as boolean_battery_level
# MAGIC      from iot_nested_data

# COMMAND ----------

# MAGIC %md Note that you are not limited to only a single `transform()` function. In fact, you can chain multiple transformation, as this 
# MAGIC code tranforms countries to both lower and upper case.

# COMMAND ----------

# MAGIC %sql select cca3,
# MAGIC      transform (cca3, c -> lcase(c)) as lower_cca3,
# MAGIC      transform (cca3, c -> ucase(c)) as upper_cca3
# MAGIC      from iot_nested_data

# COMMAND ----------

# MAGIC %md ### How to use `filter()`

# COMMAND ----------

# MAGIC %md Filter out any devices with battery levels lower than 5.

# COMMAND ----------

# MAGIC %sql select cca3, device_type, battery_level,
# MAGIC      filter (battery_level, bl -> bl < 5) as low_levels
# MAGIC      from iot_nested_data

# COMMAND ----------

# MAGIC %md ### How to use `reduce()`

# COMMAND ----------

# MAGIC %sql select cca3, device_type, battery_level,
# MAGIC      reduce(battery_level, 0, (t, acc) -> t + acc,  acc -> acc div size(battery_level) ) as average_battery_level
# MAGIC      from iot_nested_data
# MAGIC      sort by average_battery_level desc

# COMMAND ----------

# MAGIC %sql select cca3, device_type, temp,
# MAGIC      reduce(temp, 0, (t, acc) -> t + acc,  acc -> acc div size(temp) ) as average_temp
# MAGIC      from iot_nested_data
# MAGIC      sort by average_temp desc

# COMMAND ----------

# MAGIC %sql select cca3, device_type, c02_level,
# MAGIC      reduce(c02_level, 0, (t, acc) -> t + acc,  acc -> acc div size(c02_level) ) as average_c02_level
# MAGIC      from iot_nested_data
# MAGIC      sort by average_c02_level desc

# COMMAND ----------

# MAGIC %md You can combine or chain many `reduce()` functions as this code shows.

# COMMAND ----------

# MAGIC %sql select cca3, device_type, signal, temp, c02_level,
# MAGIC      reduce(signal, 0, (s, sacc) -> s + sacc,  sacc -> sacc div size(signal) ) as average_signal,
# MAGIC      reduce(temp, 0, (t, tacc) -> t + tacc,  tacc -> tacc div size(temp) ) as average_temp,
# MAGIC      reduce(c02_level, 0, (c, cacc) -> c + cacc,  cacc -> cacc div size(c02_level) ) as average_c02_level
# MAGIC      from iot_nested_data
# MAGIC      sort by average_signal desc

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

# MAGIC %md The point of this short tutorial has been to demonstrate the ease of utility of higher-order functions and lambda expressions in SQL, to work with JSON attributes nested structures and arrays. Once you have flattened or parsed the desired values into respective DataFrames or Datasets, and after saving them as a SQL view or table, you can as easily manipulate and tranform your arrays with higher-order functions in SQL as you would with DataFrame or Dataset API.
# MAGIC
# MAGIC Finally, it is easy to employ higher-order functions than to write UDFs in Python or Scala. Read the original blog on [SQL higher-order functions](https://databricks.com/blog/2017/05/24/working-with-nested-data-using-higher-order-functions-in-sql-on-databricks.html) to get more information on the _whys_.
# MAGIC