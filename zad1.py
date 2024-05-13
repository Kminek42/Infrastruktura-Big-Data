# Databricks notebook source
file_location =  "dbfs:/FileStore/tables/Nested.json"

file_type = "json"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.option("multiline","true").json(file_location)
df = df.drop("attribute1", "endGradeSeparation", "elevationAgainstDirection","formsPartOfPath" )
display(df)

# COMMAND ----------

from operator import add
from pyspark.sql.functions import concat
lista = df.rdd.map(lambda x: x[0:6]).collect()[0]
lista = [str(x) for x in lista]
print("The longest word: "+sc.parallelize(lista).fold("", lambda x, y: x if len(x) > len(y) else y))

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from pyspark.sql import Row

def drop_elements(df, field_number, drop_fields, column_name):

    lista = df.rdd.map(lambda x: x[field_number]).collect()
    output = [[str(x)  for i, x in enumerate(lista[j])  if i not in drop_fields] for j in range(len(lista))]
    output = [Row(",".join(x)) for x in output]
    
    b = spark.createDataFrame(data=output, schema = [column_name+"new",])
    
    df = df.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    b = b.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    

    final_df = df.join(b, df.row_idx == b.row_idx).\
             drop("row_idx").drop(column_name)

    final_df = final_df.withColumn(column_name, final_df[column_name+"new"]).drop(column_name+"new")

    return final_df

field_number = 6
drop_fields = [0,4,7,9,15]
column_name = "pathLinkInfo"

result = drop_elements(df, field_number, drop_fields, column_name)

display(result)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Sumowanie liczb w liście:
# MAGIC val numbers = List(1, 2, 3, 4, 5)
# MAGIC val sum = numbers.foldLeft(0)((acc, num) => acc + num)
# MAGIC
# MAGIC // Konkatenacja napisów w liście:
# MAGIC val words = List("Hello", "World", "of", "Scala")
# MAGIC val concatenated = words.foldLeft("")((acc, word) => acc + " " + word)
# MAGIC