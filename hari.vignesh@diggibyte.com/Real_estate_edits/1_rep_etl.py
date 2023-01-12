# Databricks notebook source
# MAGIC %md
# MAGIC #Note : USA Real Estate Dataset Context 

# COMMAND ----------

# MAGIC %md
# MAGIC #Context :
# MAGIC This dataset contains Real Estate listings in the US broken by State and zip code.
# MAGIC 
# MAGIC Data was collected via web scraping using python libraries

# COMMAND ----------

# MAGIC %md
# MAGIC #Inspiration
# MAGIC Can we predict housing prices based on this data?
# MAGIC 
# MAGIC Which location contains the house with the highest prices?
# MAGIC 
# MAGIC What are the correlation between house prices and other attributes?
# MAGIC 
# MAGIC What could be the trend behind housing prices?

# COMMAND ----------

# MAGIC %md
# MAGIC #WORKING ON : USA Real Estate - Predict Housing price 

# COMMAND ----------

# MAGIC %md
# MAGIC Work plan :
# MAGIC 
# MAGIC 1- Data Exploration & Analysis & Clean Data 
# MAGIC 
# MAGIC 2- Building a Machine Learning Model / classification score Volume

# COMMAND ----------

# MAGIC %md
# MAGIC #data dictionary
# MAGIC #Columns Attributes
# MAGIC The realtor-data.csv has 200k+ entries:
# MAGIC 
# MAGIC status - Housing Status (on sale or other option)
# MAGIC 
# MAGIC price - Price in USD
# MAGIC 
# MAGIC bed - Bedroom count
# MAGIC 
# MAGIC bath - Bathroom count
# MAGIC 
# MAGIC acre_lot - Acre lot
# MAGIC 
# MAGIC full_address - Full address
# MAGIC 
# MAGIC street - Street name
# MAGIC 
# MAGIC city - City name
# MAGIC 
# MAGIC state - State name
# MAGIC 
# MAGIC zip_code - Zip Code
# MAGIC 
# MAGIC house_size - House size in sqft (square feet)
# MAGIC 
# MAGIC sold_date - The date when the house is sold

# COMMAND ----------

# MAGIC %md
# MAGIC #finding (x)independent and (y)dependent columns
# MAGIC 
# MAGIC X = rest 11 colums are independent
# MAGIC 
# MAGIC y = price is only dependent

# COMMAND ----------

# MAGIC %pip install mlflow==1.20.2

# COMMAND ----------

# MAGIC %pip install seaborn==0.11.1

# COMMAND ----------

# MAGIC %pip install scikit-learn==0.24.1

# COMMAND ----------

# Import all required libraries for reading data, analysing and visualizing data

import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/data/ds/USA_Real_Estate_HariVignesh

# COMMAND ----------

data = spark.read.option('header',True).csv('/mnt/tf-abfss/data/ds/USA_Real_Estate_HariVignesh')

# COMMAND ----------

data = data.toPandas()

# COMMAND ----------

#data = pd.read_csv('realtor-data.csv')
data.head()

# COMMAND ----------

# looking the shape DataSet
data.shape

# COMMAND ----------

#checking for how much columns in datasets
data.columns

# COMMAND ----------

#Checking the dtypes of all the columns

data.info()

# COMMAND ----------

data[['price','bed','bath','acre_lot','zip_code','house_size']] = data[['price','bed','bath','acre_lot','zip_code','house_size']].apply(pd.to_numeric)

# COMMAND ----------

data.dtypes