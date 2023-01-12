// Databricks notebook source
// MAGIC %run /Users/hari.vignesh@diggibyte.com/Api_pipe_notebook/API_data_onboarding/common_schema_notebook

// COMMAND ----------

// DBTITLE 1,Passing parameters
dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("table_name", "", "table_name")
dbutils.widgets.text("database_name", "", "database_name")

val schedule_date = dbutils.widgets.get("schedule_date")
val table_name = dbutils.widgets.get("table_name")
val database_name = dbutils.widgets.get("database_name")

// COMMAND ----------

val df4 = addLoadDate(df3, schedule_date)

// COMMAND ----------

//val table_name = "Apidata11"
//val database_name = "Api_data_pipe1"
val path = "dbfs:/mnt/tf-abfss/silver/Api_data_pop/"

def savewriteWithDelta(df: DataFrame, path: String, databaseName: String, tableName: String, scheduleDate: String) {
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

  val addTableToPath = path + tableName + "/"
  df.coalesce(1).write.format("delta").mode("overWrite").option("mergeSchema", true).option("path", addTableToPath).saveAsTable(s"${database_name}.${table_name}")
}

// COMMAND ----------

savewriteWithDelta(df4, path, database_name, table_name, schedule_date)