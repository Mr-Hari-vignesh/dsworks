# Databricks notebook source
pip install DataProfiler[full]

# COMMAND ----------

pip install maven

# COMMAND ----------

from os import environ
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/bronze/indian_railways/Posts.xml

# COMMAND ----------

import json
import pandas as pd
from pandas.io.json import json_normalize

# COMMAND ----------

# MAGIC %!user/bin/pyspark --packages com.databricks:spark-xml_2.12:0.13.0

# COMMAND ----------

df = spark.read.option("multiline","true").xml("/mnt/tf-abfss/bronze/indian_railways/Posts.xml")
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books
# MAGIC USING xml
# MAGIC OPTIONS (path "/mnt/tf-abfss/bronze/indian_railways/Posts.xml", rowTag "book")

# COMMAND ----------

df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "hierarchy") \
    .option("rowTag", "att") \
    .load("/mnt/tf-abfss/bronze/indian_railways/Posts.xml")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#Flatten array of structs and structs
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType) ##---------It takes all the list of (fields) along with its (datatypes) in dictonary
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType]) ##-----it is checking whether it is                                                                                                                   (arraytype or structtype) 
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))#---to check the type of thr perticular field
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType): #---------
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

# COMMAND ----------

df_flatten = flatten(df)
display(df_flatten)

# COMMAND ----------

df_flatten.printSchema()

# COMMAND ----------

import json
from dataprofiler import Data, Profiler
import dataprofiler as dp
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql import Row

import pandas as pd
my_dataframe = spark.read.format("json").load("dbfs:/mnt/tf-abfss/bronze/indian_railways/stations.json").toPandas()
data = dp.Profiler(my_dataframe)

report  = data.report(report_options={"output_format":"pretty"})
print(json.dumps(report, indent=4))