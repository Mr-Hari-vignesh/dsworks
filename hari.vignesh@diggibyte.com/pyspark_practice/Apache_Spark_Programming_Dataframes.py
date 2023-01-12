# Databricks notebook source
# MAGIC %md
# MAGIC ** $$ Apache\ Spark\ Programming\ Dataframes $$ **

# COMMAND ----------

# MAGIC %md
# MAGIC # execute code in different language

# COMMAND ----------

# MAGIC %python
# MAGIC print("Run python")

# COMMAND ----------

# MAGIC %scala
# MAGIC print("run scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "run SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC # Run shell commands on the driver using the magic command %sh

# COMMAND ----------

ps | grep 'java'

# COMMAND ----------

# MAGIC %md
# MAGIC Render html using the function displayHTML ( avl on py,sc & R)

# COMMAND ----------

html = """<h1 style="color:blue;text-align:center;front-family:courier">Render HTML</h1>"""
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC # Access DBFS 

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/README.md

# COMMAND ----------

# MAGIC %fs mounts  

# COMMAND ----------

# MAGIC %md
# MAGIC %fs is shorthand for the DBUtils models: dbutils.fs

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

# MAGIC %md
# MAGIC Run the system commands on DBFS using DBUtills directily

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC visualize tables in the table using the databricks display function 

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC # create table

# COMMAND ----------

#%sql
#CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (paths "/mnt/databricks/mlflow-tracking");

# COMMAND ----------

# MAGIC %md
# MAGIC # Quary table and plot results

# COMMAND ----------

 #%sql
 #SELECT * FROM events

# COMMAND ----------

# MAGIC %md
# MAGIC # add notebooks parameters using widgets  

# COMMAND ----------

#spl
#CREATE widget text state default "ca"

# COMMAND ----------

# MAGIC %md
# MAGIC #creating the widget using dbutiles.widgets

# COMMAND ----------

dbutils.widgets.text("name","Rjhari","Name")
dbutils.widgets.multiselect("colors","orange",["red","orange","blue"], "Traffic sources?")

# COMMAND ----------

name = dbutils.widgets.get("name")
colors = dbutils.widgets.get("colors").split(",")

html = "<div>Hi {} ! select your colour preference.</div>".format(name)
for c in colors:
    html += """<lable for="{}" style="color:{}"><input type="radio"> {}</lable><bt>""".format(c, c, c)
    
    
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove all widgets 

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC **$$ spark\ SQL\ Demo\ and\ lab $$**

# COMMAND ----------

data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
        ]

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df = spark.createDataFrame(data)
type(df)

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md # Reader & Writer
# MAGIC  Read from CSV files
# MAGIC 1. Read from JSON files
# MAGIC 1 . Write DataFrame to files
# MAGIC 1. Write DataFrame to tables
# MAGIC 1. Write DataFrame to a Delta table 
# MAGIC 
# MAGIC #### Methods
# MAGIC  - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html" target="_blank">DataFrameReader</a>: **`csv`**, **`json`**, **`option`**, **`schema`**
# MAGIC 
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html" target="_blank">DataFrameWriter</a>: **`mode`**, **`option`**, **`parquet`**, **`format`**, **`saveAsTable`**
# MAGIC 
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html?highlight=structtype#pyspark.sql.types.StructType" target="_blank">StructType</a>: **`toDDL`**
# MAGIC ##### Spark Types
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html" target="_blank">Types</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3> DataFrameReader</h3>
# MAGIC <li>MAGIC Interface used to load a DataFrame from external storage systems</li>
# MAGIC <h4>syntax</h4>
# MAGIC <li>spark.read.parquet("path/to/files")</li>

# COMMAND ----------

# Reading file from CSV
csv_path = '/mnt/tf-abfss/data/ds/pyspark/bmw.csv'

# here we are using inferschema as True beacause we need their actual type
userdf = (spark.read.option("sep",",").option("header",True).option("inferSchema", True).csv(csv_path))

userdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a schema for types 

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, IntegerType,StructType, StructField

userdf_schema = StructType([StructField('model',StringType(),True),
                          StructField('year', DoubleType(),True),
                          StructField('price', DoubleType(), True),
                          StructField('transmission', DoubleType(), True),
                          StructField('mileage', DoubleType(), True),
                          StructField('fuelType', DoubleType(), True),
                          StructField('tax', IntegerType(), True),
                          StructField('mpg', DoubleType(), True),
                          StructField('engineSize', IntegerType(), True)])

# COMMAND ----------

# Reading file from CSV
csv_path = '/mnt/tf-abfss/data/ds/pyspark/bmw.csv'

userdf = (spark.read.option("sep",",")\
          .option("header",True)\
          .schema(userdf_schema)\
          .csv(csv_path))

userdf.printSchema()

# COMMAND ----------

# Changing the column name because, while converting type using ddl this space between the column becomes problem.

userdf.withColumnRenamed("fuelType", "fueltype")
userdf.withColumnRenamed("engineSize", "enginesize")

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema using DDl(Data definition language) syntax**

# COMMAND ----------

DDl_schema = "Date string, Open double, High double, Low double, Close double, Adj_Close double, Volume integer"

# converting the type of columns according the DDL schema
userdf = (spark.read.option("sep","")\
          .option("header",True)\
          .schema(DDl_schema)\
          .csv(csv_path))

userdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **$$ Data\ frame\ and\ column $$**

# COMMAND ----------

from pyspark.sql.functions import col

userdf.Open
userdf['Open']
col('Open')

# COMMAND ----------

# MAGIC %scala
# MAGIC $"transmission"

# COMMAND ----------

df = spark.read.csv(csv_path, header=True)

# COMMAND ----------

# Select() function
sel = df.select('year', 'model')
display(sel.head(5))

# COMMAND ----------

