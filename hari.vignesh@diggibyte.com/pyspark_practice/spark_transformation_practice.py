# Databricks notebook source
# MAGIC %md
# MAGIC **$$ Aggregation\ Functions $$**

# COMMAND ----------

# Reading file from CSV
csv_path = '/mnt/tf-abfss/data/ds/pyspark/bmw.csv'

# here we are using inferschema as True beacause we need their actual type
userdf = (spark.read.option("sep",",").option("header",True).option("inferSchema", True).csv(csv_path))

display(userdf.head(10))

# COMMAND ----------

userdf.groupBy("transmission")

# COMMAND ----------

# Groupby is having different functions
trans_count = userdf.groupBy("transmission").count()
display(trans_count.head(5))

# COMMAND ----------

#heare we getting avg price of car models
avgcarpriceDF = userdf.groupBy("model").avg("price")
display(avgcarpriceDF)

# COMMAND ----------

#here we get total price and sum of the car price by transmisssion of the model 
carpurchasequantityDF = userdf.groupBy("model","transmission").sum("price","tax")
display(carpurchasequantityDF)

# COMMAND ----------

from pyspark.sql.functions import sum
carpurchaseDF = userdf.groupby("model").agg(sum("price").alias("tax"))
display(carpurchaseDF)

# COMMAND ----------

#applay multiple aggregate function on grouped data 
from pyspark.sql.functions import avg, approx_count_distinct
carAggregatesDF = userdf.groupBy("model").agg(avg("price").alias("price of car"),approx_count_distinct("mileage").alias("car milage"))                                          
display(carAggregatesDF)                                     

# COMMAND ----------

#math operations
from pyspark.sql.functions import cos, sqrt

display(spark.range(10).withColumn("sqrt", sqrt("id")).withColumn("cos", cos("id")))

# COMMAND ----------

# MAGIC %md
# MAGIC **$$ Datetime\ Functions $$**

# COMMAND ----------

# Reading file from CSV
csv_path = '/mnt/tf-abfss/data/ds/food_inspection_dinesh/Sales_January_2019.csv'

# here we are using inferschema as True beacause we need their actual type
userdf = (spark.read.option("sep",",").option("header",True).option("inferSchema", True).csv(csv_path))

display(userdf.show(5))

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.csv(csv_path, header=True).select("Order ID",col("Order Date"))
display(df)

# COMMAND ----------

path = "/mnt/tf-abfss/data/ds/food_inspection_dinesh/tesla_stocks.csv"

stock_df = spark.read.csv(path, header=True, inferSchema=True)
display(stock_df.head(4))

# COMMAND ----------

# separating the year from date column
from pyspark.sql.functions import date_format

dt_frmt = stock_df.withColumn("year", date_format("Date", "yyyy"))
display(dt_frmt.head(4))

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, minute, second

datetimedf = (stock_df.withColumn("year", year(col("Date")))
              .withColumn("month", month(col("Date")))
              .withColumn("dayofweek", dayofweek("Date")))

display(datetimedf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting to data frame 

# COMMAND ----------

from pyspark.sql.functions import to_date

date_format = stock_df.withColumn("date",to_date(col("Date")))
display(date_format)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manipulating the date column

# COMMAND ----------

from pyspark.sql.functions import date_add

dt_add = stock_df.withColumn("plus_two_days",date_add(col("Date"),2)
)
display(dt_add)

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined function

# COMMAND ----------

def strings(strg):
    return strg[0]

strings("diggibyte")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying udf on defined function

# COMMAND ----------

apply_udf = udf(strings)

# COMMAND ----------

