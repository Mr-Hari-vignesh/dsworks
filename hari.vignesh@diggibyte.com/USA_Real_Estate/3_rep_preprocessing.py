# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/USA_Real_Estate/2_rep_eda

# COMMAND ----------

# drop duplicated value 

data.drop_duplicates(inplace=True)

# COMMAND ----------

# about na value after drop duplicated 

data.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Also , we have missing value in this column - bed, bath, house size - this columns have effect when make predict price , should be drop na

# COMMAND ----------

# drop na

data.dropna(subset=["bed","bath","house_size"],inplace=True)

# COMMAND ----------

# about na value after drop na value in this  column - bed, bath, house size

data.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC About column -sold date a lot of missing , I dont drop this and column acre_lot I dont need in predict , should drop na in column city and zip code

# COMMAND ----------

# drop na

data.dropna(subset=["city","zip_code"],inplace=True)

# COMMAND ----------

data.isnull().sum()

# COMMAND ----------

data.shape

# COMMAND ----------

# MAGIC %md
# MAGIC Now we have 71235 rows after clean data before that we had 813159, we loss around 10 % form dataset

# COMMAND ----------

#Conversion data type column - published date from object to Datetime

data["sold_date"]=pd.to_datetime(data["sold_date"])

# COMMAND ----------

# spilt special character in this columns - street and full address

data['full_address'] = data['full_address'].str.replace (r"""[^\w\s]+""","")
data['street'] = data['street'].str.replace (r"""[^\w\s]+""","")

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
# MAGIC New Jersey 15.5 %
# MAGIC 
# MAGIC New York 15 %
# MAGIC 
# MAGIC Connecticut 12 %
# MAGIC 
# MAGIC Massachusetts 8 %
# MAGIC 
# MAGIC pensilveniya 7.5%
# MAGIC 
# MAGIC New Hampshire 2.5 %
# MAGIC 
# MAGIC maine 2 %
# MAGIC 
# MAGIC Puerto Rico 2 %
# MAGIC 
# MAGIC Rhode Island 1.5 %
# MAGIC 
# MAGIC delware 1 %
# MAGIC 
# MAGIC Vermont 1 %
# MAGIC 
# MAGIC Virgin Islands 0.5 %

# COMMAND ----------

data

# COMMAND ----------

data['sold_date'].value_counts

# COMMAND ----------

# change the price by the years
sns.lineplot(data=data , x="sold_date",y="price")

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

df.drop(40599,inplace=True)

# COMMAND ----------

df.sort_values(by="house_size",ascending=False).head(2)

# COMMAND ----------

df.sort_values(by="bed",ascending=False).head(6)

# COMMAND ----------

# drop any home have above 20 bath
#df.drop([121247,510130,108951,672965],inplace=True)

# COMMAND ----------

#drop the high house size
#df.drop( 10328,inplace=True)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualization after drop some outlier value

# COMMAND ----------

# change the price by the years

sns.scatterplot(data=data , x="sold_date",y="price")

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