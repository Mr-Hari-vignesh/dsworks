// Databricks notebook source
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.http.util.EntityUtils

// COMMAND ----------

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

import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.json4s.DefaultFormats 
import org.json4s.jackson.Json
import scala.collection.mutable.LinkedHashMap
import java.net.HttpCookie
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp
import org.joda.time.DateTime
import io.delta.tables._
import org.json4s.JsonAST._
import org.apache.spark.sql.expressions._
import org.apache.http.client.methods._
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.HttpHeaders

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

// val url = "https://randomuser.me/api/"
// val client = HttpClients.createDefault()

 

// val getFlowDebugData:HttpGet = new HttpGet(url)

 

// val response:CloseableHttpResponse = client.execute(getFlowDebugData)
// val entity = response.getEntity
// val str = EntityUtils.toString(entity,"UTF-8")
// println(str)

// COMMAND ----------

val finalFormat = "yyyy-MM-dd'T'HH:mm:ss"

// COMMAND ----------

def scheduleFormat (dateTimeStr:String, dateFormat: String, finalFormat: String) = {
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val finalFormatter = DateTimeFormatter.ofPattern(finalFormat)
    val localDateTime = LocalDateTime.parse(dateTimeStr, formatter)
    localDateTime.format(finalFormatter)
}

// COMMAND ----------

val url = "https://randomuser.me/api/"
val timeout = 100
val requestConfig = RequestConfig.custom()
   .setConnectTimeout(timeout*1000)
   .setConnectionRequestTimeout(timeout*1000)
   .setSocketTimeout(timeout*1000).build()

def getApiList(url: String, config: RequestConfig): String = {
    val client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
    val get: HttpGet = new HttpGet(url)
    val response = client.execute(get)
    val responseCode = response.getStatusLine.getStatusCode
    val entity = response.getEntity
    val accounts = EntityUtils.toString(entity, "UTF-8")
    if (responseCode == 200) {
      accounts
    }
    else throw new Exception(s"Failed to fetch data from API $url, Code : $responseCode, Body : $accounts")
  }

// COMMAND ----------



// COMMAND ----------

// val df = spark.read.option("inferSchema","true").option("header","true").schema(GetOrderRandomUserSchema).json(url)

// COMMAND ----------

// MAGIC %md #Adding load date column

// COMMAND ----------

def addLoadDate(df:DataFrame, schedule_date:String):DataFrame={
     df.withColumn("load_date",to_timestamp(lit(schedule_date))) 
}

// COMMAND ----------

// MAGIC %md #Exploding the columns

// COMMAND ----------

val df1 = df.withColumn("results",explode_outer($"results")).select($"*",$"results.*",$"info.*").drop("results","info")

// COMMAND ----------

val df2 = df1.select($"gender",$"name",$"location",$"email",$"login",$"dob.*",$"registered",$"phone",$"cell",$"id",$"picture",$"nat",$"seed",$"page",$"version")
             .withColumnRenamed("age", "dob_age")
             .withColumnRenamed("date", "dob_date")
             .select($"gender",$"name.*",$"location.*",$"email",$"login.*",$"dob_age",$"dob_date",$"registered.*",$"phone",$"cell",$"id.*",$"picture.*",$"nat",$"seed",$"page",$"version").drop("name","location","login","id","picture")
             .withColumnRenamed("age", "reg_age")
             .withColumnRenamed("date", "reg_date")

// COMMAND ----------

val df3 = df2.select($"gender",$"title",$"first",$"last",$"street.*",$"city",$"state",$"country",$"postcode",$"coordinates",$"timezone",$"email",$"uuid",$"username",$"password",$"salt",$"sha1",$"sha256",$"dob_age",$"dob_date",$"reg_date",$"reg_age",$"phone",$"cell",$"value",$"large",$"large",$"medium",$"thumbnail",$"nat",$"seed",$"page",$"version")
             .withColumnRenamed("name", "street_name")
             .withColumnRenamed("number", "street_number")
             .select($"gender",$"title",$"first",$"last",$"street_name",$"street_number",$"city",$"state",$"country",$"postcode",$"coordinates.*",$"timezone.*",$"email",$"uuid",$"username",$"password",$"salt",$"sha1",$"sha256",$"dob_age",$"dob_date",$"reg_date",$"reg_age",$"phone",$"cell",$"value",$"large",$"medium",$"thumbnail",$"nat",$"seed",$"page",$"version").drop("coordinates","timezone") 
             .withColumnRenamed("latitude", "coordinates_Latitude")
             .withColumnRenamed("name", "coordinates_number")

// COMMAND ----------

