# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/safest_city/1_rep_etl

# COMMAND ----------

# importing the libraries for visualization
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# MAGIC %md #checking for unique values

# COMMAND ----------

#check for unique values
data.nunique()

# COMMAND ----------

# MAGIC %md #checking for null values

# COMMAND ----------

#use different plot to check missing values
plt.figure(figsize=(10,6))
sns.displot(
    data=data.isna().melt(value_name="missing"),
    y="variable",
    hue="missing",
    multiple="fill",
    aspect=1.25
)
plt.savefig("visualizing_missing_data_with_barplot_Seaborn_distplot.png", dpi=100)

# COMMAND ----------

# finding the missing value
data.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC It seems we have no null values

# COMMAND ----------

# MAGIC %md
# MAGIC #checking for unique categories in a categorical columns

# COMMAND ----------

data['City'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC #CHECKING FOR VALUE COUNTS

# COMMAND ----------

data['City'].value_counts()

# COMMAND ----------

data['Violent_Crime_Rate'].value_counts()

# COMMAND ----------



# COMMAND ----------

data.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualization of data

# COMMAND ----------

plt.figure(figsize=(20,7))
sns.countplot(data['City'])
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

