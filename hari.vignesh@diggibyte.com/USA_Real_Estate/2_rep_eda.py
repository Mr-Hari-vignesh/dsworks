# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/USA_Real_Estate/1_rep_etl

# COMMAND ----------

#use different plot to check missing values


# COMMAND ----------

# importing the libraries for visualization
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

#first 10 rows of the datasets
data.head(10)

# COMMAND ----------

#Last 10 rows of the datasets
data.tail(10)

# COMMAND ----------

#check for unique values

# COMMAND ----------

# MAGIC %md
# MAGIC #checking for null values

# COMMAND ----------

# finding the missing value

data.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC we have lots of missing values in the data 

# COMMAND ----------

# check if any duplicate value present in the data

data.duplicated().sum()

# COMMAND ----------

#drop it before eda 

# COMMAND ----------

# MAGIC %md
# MAGIC We have a lot of duplicate value around 70% - should drop this value

# COMMAND ----------

# MAGIC %md
# MAGIC #CHECKING FOR VALUE COUNTS

# COMMAND ----------

#do it for categorical variables

# COMMAND ----------

data['status'].value_counts()

# COMMAND ----------

data['price'].value_counts()

# COMMAND ----------

data['house_size'].value_counts()

# COMMAND ----------

data['state'].value_counts()

# COMMAND ----------

data['city'].value_counts()

# COMMAND ----------

data['street'].value_counts()

# COMMAND ----------

#finding how many object datatypes and their info
data.describe(include='object')

# COMMAND ----------

data.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC #visualization of data before preprocessing

# COMMAND ----------

# how much repeat the state  in the dataset

plt.figure(figsize=(20,7))
sns.countplot(data['state'])
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Inspecting outlayers

# COMMAND ----------

data.boxplot(figsize=(15,15))

# COMMAND ----------

#annotations

# COMMAND ----------

sns.heatmap(data.corr())

# COMMAND ----------

#finding correlation between datas
data.corr()

# COMMAND ----------

plt.figure(figsize=(10,8))
sns.countplot(data=data,x='status',hue='status')

# COMMAND ----------

plt.figure(figsize=(10,8))
sns.countplot(data=data,x='state',hue='state')