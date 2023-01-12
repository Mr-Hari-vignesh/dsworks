# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/Real_estate_edits/2_rep_eda

# COMMAND ----------

# drop duplicated value 

data.drop_duplicates(inplace=True)

# COMMAND ----------

# about na value after drop duplicated 

data.isnull().sum()

# COMMAND ----------

data.shape

# COMMAND ----------

data.head()

# COMMAND ----------

#drop all rows which have under construction 
#drop column called status
#always check for multicollinearity 
#for status is for_sale 99 percent of data

# COMMAND ----------

# look  describe data set
data.describe().round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Maximum price is (eight hundred seventy-five million)

# COMMAND ----------

data.sort_values(by="price",ascending=False).head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualizations

# COMMAND ----------

# how much repeat the state  in the dataset

plt.figure(figsize=(20,7))
sns.countplot(data['state'])
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #The highest repeat State in US :
# MAGIC 
# MAGIC New Jersey 31 %
# MAGIC 
# MAGIC New York 25 %
# MAGIC 
# MAGIC Connecticut 14 %
# MAGIC 
# MAGIC Massachusetts 10 %
# MAGIC 
# MAGIC pensilveniya 9%
# MAGIC 
# MAGIC maine 5 %
# MAGIC 
# MAGIC New Hampshire 4 %
# MAGIC 
# MAGIC Rhode Island 4 %
# MAGIC 
# MAGIC Puerto Rico 3 %
# MAGIC 
# MAGIC delware 2.5 %
# MAGIC 
# MAGIC Vermont 1 %
# MAGIC 
# MAGIC Virgin Islands 1 %

# COMMAND ----------

data

# COMMAND ----------

# change the price by the years
sns.lineplot(data=data , x="state",y="price")

# COMMAND ----------

# MAGIC %md
# MAGIC We have one house the value price 6M , but the other house the price fluctuate

# COMMAND ----------

# show the relation between number of baths and price

plt.figure(figsize=(12,7))
sns.scatterplot(data=data,x="bath",y="price")
plt.title("The relation between number of baths and price")

# COMMAND ----------

# MAGIC %md
# MAGIC we have some outlier

# COMMAND ----------

# show the relation between House size and price

plt.figure(figsize=(12,7))

sns.scatterplot(data=data,x="house_size",y="price")
plt.title("The relation between House size and price")

# COMMAND ----------

# MAGIC %md
# MAGIC also we have some value outlier

# COMMAND ----------

# MAGIC %md
# MAGIC #Now I will drop any value outlier

# COMMAND ----------

# create new dataframe for make drop
df=data

# COMMAND ----------

df.sort_values(by="house_size",ascending=False).head(2)

# COMMAND ----------

df.sort_values(by="bed",ascending=False).head(6)

# COMMAND ----------

#drop any home have above 50 bath
df.drop([536923,121247],inplace=True)

# COMMAND ----------

#drop the high house size
df.drop( 475143,inplace=True)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualization after drop some outlier value

# COMMAND ----------

# change the price by the years

sns.scatterplot(data=data , x="state",y="price")

# COMMAND ----------

# show the relation between number of bed and price

plt.figure(figsize=(12,7))
sns.scatterplot(data=data,x="bed",y="price")
plt.title("The relation between number of baths and price")

# COMMAND ----------

# show the relation between number of baths and price

plt.figure(figsize=(12,7))
sns.scatterplot(data=data,x="bath",y="price")
plt.title("The relation between number of baths and price")

# COMMAND ----------

# show the relation between House size and price

plt.figure(figsize=(12,7))

sns.scatterplot(data=data,x="house_size",y="price")
plt.title("The relation between House size and price")

# COMMAND ----------

#Make group_by city

data.groupby("state")["price"].mean().round(2).plot(kind="bar")

# COMMAND ----------

# MAGIC %md
# MAGIC #In general - When increasing number bathroom or bedrooms or home size = increasing the price

# COMMAND ----------

