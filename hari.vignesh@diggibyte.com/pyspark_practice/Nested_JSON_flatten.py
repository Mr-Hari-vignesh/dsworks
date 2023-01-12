# Databricks notebook source
# MAGIC %md
# MAGIC #pip installing Dataprofiler to our notebook 
# MAGIC 
# MAGIC (NOTE: Always pip install everything in starting of our notebook because it will restart the terminal )

# COMMAND ----------

pip install DataProfiler[full]

# COMMAND ----------

# MAGIC %md
# MAGIC #Mounting the data from microsoft Azure storage (bronze layer)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/bronze/indian_railways

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing the required library for our notebook

# COMMAND ----------

import json
import pandas as pd
from pandas.io.json import json_normalize

# COMMAND ----------

# MAGIC %md
# MAGIC #Attaching the data and viewing the Nested JSON data

# COMMAND ----------

df = spark.read.option("multiline","true").json("/mnt/tf-abfss/bronze/indian_railways/stations.json")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Orginal look of json data 

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Defining a program to flatten the Nested JSON data 

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

from pyspark.sql.functions import explode, col
df_json = df.select(explode("features").alias("ft"))\
            .select(col("ft.geometry"))\
            .withColumn("geo_cor", col("geometry.coordinates"))\
            .withColumn("geo_type", col("geometry.type"))\
            .drop("geometry")
display(df_json)

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #After flatten the Nested Json data 

# COMMAND ----------

df_flatten = flatten(df)
display(df_flatten)

# COMMAND ----------

df_flatten.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Converting spark dataframe to Pandas dataframe 
# MAGIC 
# MAGIC (why means to get a clear overview of data )(not mandatory*) i done it for my comfortness .

# COMMAND ----------

convpandas = df_flatten.toPandas()

# COMMAND ----------

convpandas.describe()

# COMMAND ----------

convpandas.describe(include="object")

# COMMAND ----------

convpandas.info()

# COMMAND ----------

convpandas.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing the required library and profiling the data 

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

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col
df1 = df.select(explode("features"))
            #.withColumn("fet_geometric", col("features_explode.geometry"))\
            #.withColumn("geo_cor", col("fet_geometric.coordinates"))\
            #.withColumn("geo_type", col("fet_geometric.type"))\
            #.drop("geometry")
display(df1)

# COMMAND ----------

df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/hari.vignesh@diggibyte.com/stations.json")

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import explode, col
df2 = df1.select(explode("features").alias("ft"))\
         .withColumn("fet_geometric", col("ft.geometry"))\
         .withColumn("geo_cor", col("fet_geometric.coordinates"))\
         .withColumn("geo_type", col("fet_geometric.type"))\
         .drop("ft")
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col
df3 = df1.select(explode("features").alias("ft"))\
         .withColumn("fet_properties", col("ft.properties"))
display(df3)

# COMMAND ----------

