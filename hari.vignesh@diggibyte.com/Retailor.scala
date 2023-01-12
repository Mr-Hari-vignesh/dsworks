// Databricks notebook source
// MAGIC %md
// MAGIC #Importing all the necessary functions 

// COMMAND ----------

import spark.sqlContext.implicits._
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame}
import io.delta.tables._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.regexp_replace
import scala.util.Try
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{countDistinct,dayofmonth , month, year}

// COMMAND ----------

val data = spark.read.format("json").json("/mnt/bronze/data_d934ec56-4901-4a1e-97f9-9ad322cae957_691ede08-0aa2-4cd9-b531-b4fb6cb0f2a6.json")

// COMMAND ----------


// #data = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tf-abfss/data/ds/USA_Real_Estate_HariVignesh/realtor-data.csv")
// #display(data)

// COMMAND ----------


// #create mount point for silver
// ##dbutils.fs.mount(
// #    source = "wasbs://cntexapure@stexapure.blob.core.windows.net/silver/",
// #    mount_point = '/mnt/cntexapure1',
// #    extra_configs = {'fs.azure.account.key.stexapure.blob.core.windows.net':'a5uL2nMTajY7DKEZ8NiidhZr16dAMZGIum3CdvuIK5YRxKA88ovBo/SEOSgrOowGQzDETZw6CMFoHVj1JmNdwA=='})

// COMMAND ----------


//#silverdf = data.write.mode("append").format("csv").option("header",True).save("/mnt/cntexapure1")

// COMMAND ----------


// #%py
// #dbutils.fs.unmount("/mnt/cntexapure")

// COMMAND ----------

//dbutils.fs.ls("/mnt/cntexapure")

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType, DoubleType, LongType};

// COMMAND ----------

// DBTITLE 1,Schema for Retailor data 
val RetailorSchema = new (StructType)
  .add("price",StringType)
  .add("bed",FloatType)
  .add("bath",FloatType)
  .add("acre_lot",FloatType)
  .add("full_address",StringType)
  .add("street",StringType)
  .add("city",StringType)
  .add("state",StringType)
  .add("zip_code",StringType)
  .add("house_size",StringType)

// COMMAND ----------

 val df = spark.read
         .format("csv")
         .option("header", "true")
         .schema(RetailorSchema)
         .load("dbfs:/mnt/tf-abfss/data/ds/USA_Real_Estate_HariVignesh/realtor-data.csv")

// COMMAND ----------

display(df)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.to_timestamp



// COMMAND ----------

def addLoadDate(df:DataFrame, scheduleDate:String):DataFrame={
     df.withColumn("load_date",to_timestamp(lit(scheduleDate))) 
}

// COMMAND ----------

//val schedule_date = "2022-12-21"

// COMMAND ----------

//val df1 = addLoadDate(df, schedule_date)

// COMMAND ----------

//display(df1)

// COMMAND ----------

dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("tableName", "", "tableName")
dbutils.widgets.text("databaseName", "", "databaseName")
//dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")

val scheduleDate = dbutils.widgets.get("schedule_date")
val tableName = dbutils.widgets.get("tableName")
val databaseName = dbutils.widgets.get("databaseName")
//val data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")

// COMMAND ----------

//val tableName = "Retailordata1"
//val databaseName = "PractiseRetailor"
val path = "dbfs:/mnt/tf-abfss/silver/RetailorDataPractise"

def savewriteWithDelta(df: DataFrame, path: String, databaseName: String, tableName: String, scheduleDate: String) {
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

  val addTableToPath = path + tableName + "/"
  df.coalesce(1).write.format("delta").mode("overWrite").option("mergeSchema", true).option("path", addTableToPath).saveAsTable(s"${databaseName}.${tableName}")
}

// COMMAND ----------

savewriteWithDelta(df, path, databaseName, tableName, scheduleDate)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from PractiseRetailor.Retailordata1

// COMMAND ----------

