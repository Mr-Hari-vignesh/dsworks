# Databricks notebook source
# MAGIC %md #Note : safest city to live-in dataset

# COMMAND ----------

# MAGIC %md
# MAGIC #Context :
# MAGIC This dataset contains Real Estate listings in the Safest Cities in America: The Cost of Crime in Our Communities
# MAGIC 
# MAGIC it's an self created dataset

# COMMAND ----------

# MAGIC %md #Insperation
# MAGIC 
# MAGIC 1.How do you define safety in a city or community? Are there factors beyond crime rates?
# MAGIC 
# MAGIC 2.How does the correlation between crime rates and income factor in our assessment of the safety of communities? Should it?
# MAGIC 
# MAGIC 3.How does crime impact a community's economic well-being? How about individual wealth?
# MAGIC 
# MAGIC 4.What programs, strategies or interventions have been shown to reduce crime or improve real or perceived safety in communities?

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Dictonary :
# MAGIC 
# MAGIC This dataset contains 6 colums and 316 Entries :
# MAGIC 
# MAGIC columns:
# MAGIC 
# MAGIC Independent colums (x) :
# MAGIC 1.
# MAGIC 2.Crime Cost per Capita 
# MAGIC 3.
# MAGIC 4.
# MAGIC 5.

# COMMAND ----------

# MAGIC %md #Working-on
# MAGIC Real estate - To analize safest city in america by crime data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/data/ds/safest_city

# COMMAND ----------

# Import all required libraries for reading data, analysing and visualizing data

import numpy as np
import pandas as pd

# COMMAND ----------

data = spark.read.option('header',True).csv('/mnt/tf-abfss/data/ds/safest_city')

# COMMAND ----------

data = data.toPandas()

# COMMAND ----------

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

#changing object to numeric
data[['Crime_Cost_per_Capita','Violent_Crime_Rate','Property_Crime_Rate','Cost_of_Crime_($000s)','Population']] = data[['Crime_Cost_per_Capita','Violent_Crime_Rate','Property_Crime_Rate','Cost_of_Crime_($000s)','Population']].apply(pd.to_numeric)

# COMMAND ----------

data.info()

# COMMAND ----------

