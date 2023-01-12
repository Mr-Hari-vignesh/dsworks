// Databricks notebook source
// MAGIC %run /Users/hari.vignesh@diggibyte.com/Api_pipe_notebook/API_data_onboarding/API_source_to_bronze/common_functions

// COMMAND ----------

// dbutils.widgets.text("schedule_date","","schedule_date")
// dbutils.widgets.text("target_table_name", "", "target_table_name")
// dbutils.widgets.text("data_base_uri", "", "data_base_uri")
// dbutils.widgets.text("source_name", "", "source_name")
dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("table_name", "", "table_name")
dbutils.widgets.text("database_name", "", "database_name")
dbutils.widgets.text("url", "", "url")

val schedule_date = dbutils.widgets.get("schedule_date")
val table_name = dbutils.widgets.get("table_name")
val database_name = dbutils.widgets.get("database_name")
val url = dbutils.widgets.get("url")

// val schedule_date = dbutils.widgets.get("schedule_date")
// val target_table_name = dbutils.widgets.get("target_table_name")
// val data_base_uri = dbutils.widgets.get("data_base_uri")//"wasbs://cntexapure@stexapure.blob.core.windows.net" 
// val source_name = dbutils.widgets.get("source_name")
//val url = dbutils.widgets.get("url")

// COMMAND ----------

val timeout = 100
val requestConfig = RequestConfig.custom()
   .setConnectTimeout(timeout*1000)
   .setConnectionRequestTimeout(timeout*1000)
   .setSocketTimeout(timeout*1000).build()

// COMMAND ----------

getApiList(url,requestConfig)

// COMMAND ----------

getApiList(url,requestConfig)

// COMMAND ----------

val path = "dbfs:/mnt/tf-abfss/bronze/test_nb/"
val collectionDf = List(getApiList(url,requestConfig))
val collectionFinalDf = collectionDf.toDF("collectionDf")
val outputfinaldf = addLoadDate(collectionFinalDf, schedule_date)

// COMMAND ----------

display(outputfinaldf)

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/cntexapure/bronze/Api_data_pipe/random_user/part-00000-tid-6430164776399257292-3c36c473-e6fd-4315-85d9-24e0ed71b6f2-12-1-c000.json

// COMMAND ----------

display(spark.read.json("dbfs:/mnt/cntexapure/bronze/Api_data_pipe/random_user/part-00000-tid-6430164776399257292-3c36c473-e6fd-4315-85d9-24e0ed71b6f2-12-1-c000.json"))

// COMMAND ----------

def savewriteWithDelta(df: DataFrame, path: String, database_name: String, table_name: String, schedule_date: String) {
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $database_name")

  val addTableToPath = path + table_name + "/"
  df.coalesce(1).write.format("delta").mode("overWrite").option("mergeSchema", true).option("path", addTableToPath).saveAsTable(s"${database_name}.${table_name}")
}

// COMMAND ----------

savewriteWithDelta(outputfinaldf, path, database_name, table_name, schedule_date)

// COMMAND ----------

