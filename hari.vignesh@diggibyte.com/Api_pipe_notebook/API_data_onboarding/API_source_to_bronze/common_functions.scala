// Databricks notebook source
// #  %python
// # dbutils.fs.mount(
// #   source = "wasbs://cntexapure@stexapure.blob.core.windows.net/",
// #   mount_point = "/mnt/cntexapure",
// #   extra_configs = {"fs.azure.account.key.stexapure.blob.core.windows.net":"a5uL2nMTajY7DKEZ8NiidhZr16dAMZGIum3CdvuIK5YRxKA88ovBo/SEOSgrOowGQzDETZw6CMFoHVj1JmNdwA=="})

// COMMAND ----------

//%fs ls /mnt/cntexapure/bronze/Api_data_pipe/random_user/

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
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.HttpHeaders

// COMMAND ----------

def addLoadDate(df:DataFrame, schedule_date:String):DataFrame={
     df.withColumn("load_date",to_timestamp(lit(schedule_date))) 
}

// COMMAND ----------

// val url = "https://randomuser.me/api/"
val timeout = 100
val requestConfig = RequestConfig.custom()
   .setConnectTimeout(timeout*1000)
   .setConnectionRequestTimeout(timeout*1000)
   .setSocketTimeout(timeout*1000).build()

def getApiList(url: String, config: RequestConfig): String = {
    val client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
    val get = new HttpGet(url)
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

// val get: HttpGet = new HttpGet("https://randomuser.me/api/")