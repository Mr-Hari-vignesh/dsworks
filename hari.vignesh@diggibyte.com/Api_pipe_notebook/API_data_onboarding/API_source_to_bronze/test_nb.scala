// Databricks notebook source
// import org.apache.commons.io.IOUtils
// import java.net.URL

// COMMAND ----------

//import scalaj.http._
//import scalaj.collection.Imports._
import org.scalaj:scalaj-http_2.12:2.4.1
 

// COMMAND ----------



// COMMAND ----------

val url = new URL("https://randomuser.me/api/")
val testjson = IOUtils.toString(url,"UTF-8").lines.toList.toDS()


// COMMAND ----------

display(testjson)

// COMMAND ----------

val data = spark.read.json(testjson)

// COMMAND ----------

display(data)

// COMMAND ----------

data.write.json("dbfs:/mnt/tf-abfss/bronze/test_nb")

// COMMAND ----------

val data4 = spark.read.json("dbfs:/mnt/tf-abfss/bronze/test_nb/part-00000-tid-7489299715037194359-5248cb3c-4547-4571-ab46-fbb5f5ba5f4f-36-1-c000.json")

// COMMAND ----------

