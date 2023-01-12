// Databricks notebook source
// DBTITLE 1,Import functions lib
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

// MAGIC %md #Common Schema 

// COMMAND ----------

// DBTITLE 1,Name schema
val NameSchema: StructType = new (StructType)
  .add("title",StringType)
  .add("first",StringType)
  .add("last",StringType)

// COMMAND ----------

// DBTITLE 1,Location Schema
val TimezoneSchema: StructType = new (StructType)
  .add("offset",StringType)
  .add("description",StringType)

val CoordinatesSchema: StructType = new (StructType)
  .add("latitude",StringType)
  .add("name",StringType)

val StreetSchema: StructType = new (StructType)
  .add("number",LongType)
  .add("name",StringType)

val LocationSchema: StructType = new (StructType)
  .add("street",StreetSchema)
  .add("city",StringType)
  .add("state",StringType)
  .add("country",StringType)
  .add("postcode",StringType)
  .add("coordinates",CoordinatesSchema)
  .add("timezone",TimezoneSchema)
  

// COMMAND ----------

// DBTITLE 1,DOB schema
val DobSchema: StructType = new (StructType)
  .add("date",StringType)
  .add("age",IntegerType)

// COMMAND ----------

// DBTITLE 1,registered Schema 
val RegisteredSchema: StructType = new (StructType)
  .add("date",StringType)
  .add("age",IntegerType)

// COMMAND ----------

// DBTITLE 1,ID schema
val IdSchema: StructType = new (StructType)
  .add("name",StringType)
  .add("value",StringType)

// COMMAND ----------

// DBTITLE 1,Picture schema
val PictureSchema: StructType = new (StructType)
  .add("large",StringType)
  .add("medium",StringType)
  .add("thumbnail",StringType)

// COMMAND ----------

// DBTITLE 1,Login schema
val LoginSchema: StructType = new (StructType)
  .add("uuid",StringType)
  .add("username",StringType)
  .add("password",StringType)
  .add("salt",StringType)
  .add("sha1",StringType)
  .add("sha256",StringType)
  

// COMMAND ----------

// DBTITLE 1,child schema 
val ResultSchema: StructType = new (StructType)
  .add("gender",StringType)
  .add("name",NameSchema)
  .add("location",LocationSchema)
  .add("email",StringType)
  .add("login",LoginSchema)
  .add("dob",DobSchema)
  .add("registered",RegisteredSchema)
  .add("phone",StringType)
  .add("cell",StringType)
  .add("id",IdSchema)
  .add("picture",PictureSchema)
  .add("nat",StringType)


// COMMAND ----------

// DBTITLE 1,Info schema
val InfoSchema: StructType = new (StructType)
  .add("seed",StringType)
  .add("results",IntegerType)
  .add("page",IntegerType)
  .add("version",StringType)


// COMMAND ----------

// DBTITLE 1,Parent schema 
val GetOrderRandomUserSchema: StructType = new (StructType)
  .add("results",ArrayType(ResultSchema))
  .add("info",InfoSchema)

// COMMAND ----------

// MAGIC %md #Reading the data from bronze layer 

// COMMAND ----------

val df = spark.read.option("inferSchema","true").option("header","true").schema(GetOrderRandomUserSchema).json("dbfs:/mnt/tf-abfss/bronze/data_559c9b1c-3fc1-456b-bc1f-3bf52f2b73d5_48e57423-2057-430d-a94a-010d72d6812b.json")

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md #Adding load date column

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.to_timestamp


// COMMAND ----------

// MAGIC %md #Exploding the columns

// COMMAND ----------

// MAGIC %md #DF-1

// COMMAND ----------

val df1 = df.withColumn("results",explode_outer($"results")).select($"*",$"results.*",$"info.*").drop("results","info")

// COMMAND ----------

display(df1)

// COMMAND ----------

// MAGIC %md #DF-2

// COMMAND ----------

val df2 = df1.select($"gender",$"name",$"location",$"email",$"login",$"dob.*",$"registered",$"phone",$"cell",$"id",$"picture",$"nat",$"seed",$"page",$"version")
             .withColumnRenamed("age", "dob_age")
             .withColumnRenamed("date", "dob_date")
             .select($"gender",$"name.*",$"location.*",$"email",$"login.*",$"dob_age",$"dob_date",$"registered.*",$"phone",$"cell",$"id.*",$"picture.*",$"nat",$"seed",$"page",$"version").drop("name","location","login","id","picture")
             .withColumnRenamed("age", "reg_age")
             .withColumnRenamed("date", "reg_date")

// COMMAND ----------

display(df2)

// COMMAND ----------

// MAGIC %md #DF-3

// COMMAND ----------

val df3 = df2.select($"gender",$"title",$"first",$"last",$"street.*",$"city",$"state",$"country",$"postcode",$"coordinates",$"timezone",$"email",$"uuid",$"username",$"password",$"salt",$"sha1",$"sha256",$"dob_age",$"dob_date",$"reg_date",$"reg_age",$"phone",$"cell",$"value",$"large",$"large",$"medium",$"thumbnail",$"nat",$"seed",$"page",$"version")
             .withColumnRenamed("name", "street_name")
             .withColumnRenamed("number", "street_number")
             .select($"gender",$"title",$"first",$"last",$"street_name",$"street_number",$"city",$"state",$"country",$"postcode",$"coordinates.*",$"timezone.*",$"email",$"uuid",$"username",$"password",$"salt",$"sha1",$"sha256",$"dob_age",$"dob_date",$"reg_date",$"reg_age",$"phone",$"cell",$"value",$"large",$"medium",$"thumbnail",$"nat",$"seed",$"page",$"version").drop("coordinates","timezone") 
             .withColumnRenamed("latitude", "coordinates_Latitude")
             .withColumnRenamed("name", "coordinates_number")

// COMMAND ----------

display(df3)

// COMMAND ----------

def addLoadDate(df:DataFrame, schedule_date:String):DataFrame={
     df.withColumn("load_date",to_timestamp(lit(schedule_date))) 
}

// COMMAND ----------

// MAGIC %md #Passing parameters 

// COMMAND ----------

dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("table_name", "", "table_name")
dbutils.widgets.text("database_name", "", "database_name")

val schedule_date = dbutils.widgets.get("schedule_date")
val table_name = dbutils.widgets.get("table_name")
val database_name = dbutils.widgets.get("database_name")

// COMMAND ----------

// MAGIC %md #DF-4

// COMMAND ----------

val df4 = addLoadDate(df3, schedule_date)

// COMMAND ----------

display(df4)

// COMMAND ----------

// MAGIC %md #Saving the data from bronze to silver

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

// COMMAND ----------

// %sql
// select * from Api_data_pipe.Apidata11

// COMMAND ----------

// %sql
// Show Databases 