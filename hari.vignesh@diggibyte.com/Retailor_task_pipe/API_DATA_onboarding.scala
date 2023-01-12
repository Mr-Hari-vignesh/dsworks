// Databricks notebook source
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.HttpResponse
import org.slf4j.LoggerFactory
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.JsonMethods.{compact, render}
//import com.softwaremill.sttp._
import java.io._
import org.apache.spark.sql.types._
import java.util.zip.{ZipEntry, ZipInputStream}
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods._
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import java.util.Base64

// COMMAND ----------

val timeout = 60
val logger = LoggerFactory.getLogger(getClass.getSimpleName)

 val requestConfig = RequestConfig.custom()
   .setConnectTimeout(timeout*1000)
   .setConnectionRequestTimeout(timeout*1000)
   .setSocketTimeout(timeout*1000).build()


// COMMAND ----------

def getApi(url: String, token: String, config: RequestConfig) = {
  val client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
    val get:HttpGet = new HttpGet(url)
    get.addHeader("Authorization", s"Bearer ${token}")
    get.addHeader("Content-Type", "application/json")
    val response = client.execute(get)
    val entity = response.getEntity
  EntityUtils.toString(entity, "UTF-8")
}

// COMMAND ----------

//def getApi(url: String, token: String, config: RequestConfig)

// COMMAND ----------

def readSurvey(surveyName:String,scheduleDate:String,dataLakeBaseUri:String)=
{
 val surveyPath = s"dbfs:/tmp/qualtrics/$surveyName"
 val bronzeLayerPath = dataLakeBaseUri+s"/bronze/vtex_latam/$surveyName/$scheduleDate/"
 val df = spark.read.option("header","true").option("mode", "DROPMALFORMED").csv(surveyPath)
 val zipIndex = df.rdd.zipWithIndex().map{
    case(row, index) => Row.fromSeq(row.toSeq :+ index)
  }

 val newSchema = df.schema.add("index", LongType, false)
 val finalDf = spark.createDataFrame(zipIndex, newSchema).filter($"index" =!= 0).drop("index")
 finalDf.write.mode("overwrite").csv(bronzeLayerPath)
 display(finalDf)
}

// COMMAND ----------

// MAGIC %python
// MAGIC import requests 

// COMMAND ----------

// MAGIC %python
// MAGIC response = requests.get("http://universities.hipolabs.com/search?country=United+States")
// MAGIC print(response)

// COMMAND ----------



// COMMAND ----------

