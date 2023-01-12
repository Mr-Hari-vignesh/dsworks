# Databricks notebook source
# MAGIC %md
# MAGIC #used to drop the exesting table

# COMMAND ----------

#%sql
#DROP TABLE table1

# COMMAND ----------

#%sql
#DROP TABLE table2

# COMMAND ----------

# MAGIC %md
# MAGIC #creating the Table 1 (product_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE table1 (
# MAGIC     Product_Name varchar(50),
# MAGIC     Issue_Date long,
# MAGIC     Price int,
# MAGIC     Brand varchar(20),
# MAGIC     Country varchar(20),
# MAGIC     product_number int
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO table1 VALUES("Washing Machine", 1648770933000, 20000, "Samsung", "India", 0001),
# MAGIC                           ("Refrigerator", 1648770999000, 35000, "  LG", "null", 0002),
# MAGIC                           ("Air Cooler", 1648770948000, 45000, "  Voltas", "null", 0003)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1

# COMMAND ----------

# MAGIC %md
# MAGIC #changing table 1 to (pandas format)

# COMMAND ----------

df = _sqldf.toPandas()

# COMMAND ----------

df

# COMMAND ----------

from datetime import datetime as dt

# COMMAND ----------

def timestamp_table1 (col):
    df[col] = df[col].apply(lambda x:  dt.fromtimestamp(x/1000))
    return df[col]

# COMMAND ----------

timestamp_table1("Issue_Date")

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df['Date'] = pd.to_datetime(df['Issue_Date']).dt.date
df

# COMMAND ----------

# MAGIC %md
# MAGIC #Defining a function to replace null --> Empty

# COMMAND ----------

def removing_null(col):
    df[col] = df[col].apply(lambda x: x.replace("null"," ") if "null" in x else x)
    return df[col]

# COMMAND ----------

removing_null("Country")

# COMMAND ----------

# MAGIC %md
# MAGIC #Defining a function to remove a space

# COMMAND ----------

def removing_space(col):
    df[col] = df[col].apply(lambda x: x.strip())
    return df[col]

# COMMAND ----------

removing_space("Brand")

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %md
# MAGIC Change the camel case columns to snake case  
# MAGIC Example: SourceId: source_id, TransactionNumber: transaction_number

# COMMAND ----------

import re
def str_uppercase_split():
    column = df.columns.to_list()
    for i in column:
        column_split = re.findall("[a-zA-Z][^A-Z]*",i)
        underscore_split = "_".join(column_split).lower()
        a = df.rename(columns= {i:underscore_split}, inplace=True)
    return df

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %md
# MAGIC #creating the table 2 (transaction data)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE table2 (
# MAGIC   SourceId int,
# MAGIC   TransactionNumber int,
# MAGIC   Language varchar(10),
# MAGIC   ModelNumber int,
# MAGIC   StartTime varchar(40),
# MAGIC   ProductNumber int
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO table2 VALUES (150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 0001),
# MAGIC                                     (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 0002),
# MAGIC                                     (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 0003)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table2

# COMMAND ----------

def split_on_uppercase(col):
    
    return [x for x in s.split(' ') if x.isupper()]

# COMMAND ----------

# MAGIC %md
# MAGIC #changing table 2 to (pandas format)

# COMMAND ----------

df1 = _sqldf.toPandas()

# COMMAND ----------

df1

# COMMAND ----------

# MAGIC %md
# MAGIC B) Add another column as start_time_ms and convert the values of StartTime to milliseconds. 
# MAGIC Example:  
# MAGIC 
# MAGIC Input: 2021-12-27T08:20:29.842+0000 -> Output: 1640593229842 
# MAGIC 
# MAGIC Input: 2021-12-27T08:21:14.645+0000 -> Output: 1640593274645 
# MAGIC 
# MAGIC Input: 2021-12-27T08:22:42.445+0000 -> Output: 1640593362445 
# MAGIC 
# MAGIC Input: 2021-12-27T08:22:43.183+0000 -> Output: 1640593363183 

# COMMAND ----------

import re
column = df1.columns.to_list()
def split_on_uppercase():
    for i in column:
        column_split = re.findall("[a-zA-Z][^A-Z]*",i)
        underscore_split = "_".join(column_split).lower()
        df1.rename(columns= {i:underscore_split}, inplace=True)
    return df1

# COMMAND ----------

split_on_uppercase()

# COMMAND ----------

# MAGIC %md
# MAGIC #b) Add another column as start_time_ms and convert the values of StartTime to milliseconds.

# COMMAND ----------

df1['start_time'] = pd.to_datetime(df1['start_time'])

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

df1['start_time']

# COMMAND ----------

def date_timestamp(col):
    for i in df1[col]:
        df1["{}_ms".format(col)] = datetime.timestamp(i)
    return df1

# COMMAND ----------

date_timestamp("start_time")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Combine both the tables based on the Product Number
# MAGIC 
# MAGIC • and get all the fields in return.
# MAGIC 
# MAGIC • And get the country as EN

# COMMAND ----------

df_final = pd.merge(df1, df, on='product_number').set_index("product_number")
df_final

# COMMAND ----------

df_final[df_final['language'] == "EN"]