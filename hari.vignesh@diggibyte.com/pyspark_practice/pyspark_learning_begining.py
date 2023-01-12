# Databricks notebook source
# MAGIC %md
# MAGIC Creating Pyspark Session

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

# creating a spark session
spark = SparkSession.builder.appName("pyspark practice").getOrCreate()

display(spark)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/data/ds/pyspark/bmw.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Reading a csv file

# COMMAND ----------

csv_file = '/mnt/tf-abfss/data/ds/pyspark/bmw.csv'
df = spark.read.csv(csv_file)

# COMMAND ----------

# displaying the dataframe without header is true
display(df.show(5))

# COMMAND ----------

data = spark.read.option('header',True).csv('/mnt/tf-abfss/data/ds/pyspark/bmw.csv')
display(data.show(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a dataframe with header is true

# COMMAND ----------

# printing the schema of the data
data = spark.read.csv(
    '/mnt/tf-abfss/data/ds/pyspark/bmw.csv',
    sep = ',',
    header = True,
    )

data.printSchema()

# COMMAND ----------

# checking the data types
data.dtypes

# COMMAND ----------

# getting the first 3 rows of data from spark dataframe
data.head(3)

# COMMAND ----------

data.show(2)

# COMMAND ----------

data.first()

# COMMAND ----------

data.describe().show()

# COMMAND ----------

data.columns

# COMMAND ----------

data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a new column as stock_Date with the reference of Date column

# COMMAND ----------

data = data.withColumn('Year_of_purchase', data.year)

data.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming the column

# COMMAND ----------

data = data.withColumnRenamed('Year_of_purchase', 'year_of_purchase')

data.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping the extra created colun from the spark dataframe

# COMMAND ----------

# after dropping the added column we are having the existing columns
data = data.drop('year_of_purchas')

data.show(10)

# COMMAND ----------

##threshold
data.na.drop(how="any",thresh=3).show(2)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %md
# MAGIC checking the null values

# COMMAND ----------

data.filter(col("price").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Imputting the missing values

# COMMAND ----------

# Remove Rows with Missing Values

data.na.drop()
data.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting the single column and multiple column

# COMMAND ----------

## Selecting Single Column

data.select('mileage').show(5)

## Selecting Multiple columns

data.select(['fuelType','mileage', 'mpg', 'engineSize']).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering the column

# COMMAND ----------

from pyspark.sql.functions import col, lit

data.filter((col('mpg') >= lit(20)) | (col('mpg') <= lit(45))).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering the data by using the between condition

# COMMAND ----------

## fetch the data where the adjusted value is between 100.0 and 500.0

data.filter(data.year.between(2016, 2019)).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering the data by using the When condition

# COMMAND ----------

from pyspark.sql import functions as f
data.select('model', 'price','transmission', 
            f.when(data.engineSize >= 2.0, 1).otherwise(0)
           ).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering the data by using the like condition

# COMMAND ----------

data.select('model','engineSize', 
            data.engineSize.rlike('^[3]').alias('Engine size that with 3')
            ).distinct().show(5)

# COMMAND ----------

