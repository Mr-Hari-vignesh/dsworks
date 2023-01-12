# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/Real_estate_edits/1_rep_etl

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
data.nunique()

# COMMAND ----------

# MAGIC %md
# MAGIC #checking for null values

# COMMAND ----------

# finding the missing value

data.isnull().sum()

# COMMAND ----------

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

# MAGIC %md
# MAGIC we have lots of missing values in the data 

# COMMAND ----------

# check if any duplicate value present in the data

data.duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC We have a lot of duplicate value around 70% - should drop this value
# MAGIC 
# MAGIC Oops thats alot of duplicate values! We have no choice but to drop those as it might cause overfitting on our data.

# COMMAND ----------

data.drop_duplicates(inplace=True)
data.shape

# COMMAND ----------

# MAGIC %md
# MAGIC While analyzing the data, I think it would be better just to drop sold_date, street and full address because those columns might not help on regression

# COMMAND ----------

data = data.drop(columns=['sold_date', 'street', 'full_address'])
data.head()

# COMMAND ----------

#now checking for missing values 
data.isnull().sum()

# COMMAND ----------

data.dropna().shape

# COMMAND ----------

# MAGIC %md
# MAGIC Its not gonna be a great idea to just drop missing values considering that we had drop around 90% of our data because of duplicate values and if we drop missing values, our rows will just be 13062. The alternative solution that we can do is to impute the missing values on their median (if numerical value) and mode (if non-numerical value).
# MAGIC 
# MAGIC But I think we should use the dataset that was dropped missing values on exploratory data analysis so that we can analyze real data

# COMMAND ----------

data_nonull = data.dropna()

# COMMAND ----------

data['price'] = data['price'].fillna(data['price'].median())
data['bed'] = data['bed'].fillna(data['bed'].median())
data['bath'] = data['bath'].fillna(data['bath'].median())
data['acre_lot'] = data['acre_lot'].fillna(data['acre_lot'].median())
data['city'] = data['city'].fillna(data['city'].mode()[0])
data['zip_code'] = data['zip_code'].fillna(data['zip_code'].median())
data['house_size'] = data['house_size'].fillna(data['house_size'].median())

# COMMAND ----------

# MAGIC %md
# MAGIC #checking for unique categories in a categorical columns

# COMMAND ----------

data['status'].unique()

# COMMAND ----------

data['city'].unique()

# COMMAND ----------

data['state'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC #converting catogarical colum to num

# COMMAND ----------

status_label = {"for_sale":0, "ready_to_build":1}
data['status']=data['status'].apply(lambda x: status_label[x])
data['status'].head(2)

# COMMAND ----------

data['status'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC #CHECKING FOR VALUE COUNTS

# COMMAND ----------

# MAGIC %md
# MAGIC do it for categorical variables

# COMMAND ----------

data['status'].value_counts()

# COMMAND ----------

data['state'].value_counts()

# COMMAND ----------

data.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC #visualization of data 

# COMMAND ----------

# MAGIC %md
# MAGIC #Exploratory Data Analysis
# MAGIC Here we will use plotly for interactive data analysis. Also, we are using the dataset that we dropped missing values so that we would analyze real data.

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if number of bed, bath, and house size affects its price

# COMMAND ----------

#HOUSE SIZE AND PRICE
sns.lineplot(data = data, x='house_size', y='price')
display()

# COMMAND ----------

# MAGIC %md
# MAGIC #totalsquare feet and its prize

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the graph above, it seems that theres a mistake here, I think it would be crazy if someone would sell a 1 million square feet house and just sell it for 8 million, so we need to remove that and also the 60 million house as its so high and its definetly an outlier

# COMMAND ----------

data_nonull = data_nonull.sort_values(by='house_size', ascending=False)
data_nonull = data_nonull.drop(10328)
data = data.drop(10328)

# COMMAND ----------

data_nonull = data_nonull.sort_values(by='price', ascending=False)
data_nonull = data_nonull.drop(40599)
data = data.drop(40599)

# COMMAND ----------

sns.lineplot(data = data, x='house_size', y='price')
display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Tota squarefeet and its price after removing 

# COMMAND ----------

# MAGIC %md
# MAGIC Bed & Price

# COMMAND ----------

sns.lineplot(data = data, x='bed', y='price')
display()

# COMMAND ----------

# MAGIC %md
# MAGIC Bath & Price

# COMMAND ----------

sns.lineplot(data = data, x='bath', y='price')
display()

# COMMAND ----------

# MAGIC %md
# MAGIC By analyzing the graphs above, we can say yes. Yes, the number of beds, baths, and house size does affects its price. All of them has a positive correlation towards price but its a weak correlations.

# COMMAND ----------

# MAGIC %md
# MAGIC #Now lets rank which state has the highest median house prices

# COMMAND ----------

order = data_nonull.groupby(by=['state'])['price'].median().sort_values(ascending=False).index
fig = sns.lineplot(data = data, x='state', y='price')
fig = plt.xticks(rotation=90)
plt.show()
display()


# COMMAND ----------

# how much repeat the state  in the dataset

plt.figure(figsize=(20,7))
sns.countplot(data['state'])
plt.xticks(rotation=90)
plt.show()
display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Inspecting outlayers

# COMMAND ----------

data.boxplot(figsize=(15,15))

# COMMAND ----------

#annotations

# COMMAND ----------

#heatmap
sns.heatmap(data.corr(),annot = True)
display()

# COMMAND ----------

#finding correlation between datas
data.corr()

# COMMAND ----------

# MAGIC %md
# MAGIC  it seems that our price columns has weak correlation towards to bed but has moderate correlations to number of baths and house size which is great, it might help us to get a decent r2 score

# COMMAND ----------

plt.figure(figsize=(10,8))
sns.countplot(data=data,x='status',hue='status')
display()

# COMMAND ----------

plt.figure(figsize=(10,8))
sns.countplot(data=data,x='state',hue='state')
plt.xticks(rotation=90)
display()

# COMMAND ----------

