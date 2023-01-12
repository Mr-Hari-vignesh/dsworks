# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/USA_Real_Estate/3_rep_preprocessing

# COMMAND ----------

#Importing the basic librarires for building model


from sklearn.linear_model import LinearRegression  
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error ,mean_squared_error, median_absolute_error,confusion_matrix,accuracy_score,r2_score

from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import StandardScaler ,PolynomialFeatures,minmax_scale,MaxAbsScaler ,LabelEncoder,MinMaxScaler

from sklearn.ensemble import RandomForestRegressor

# COMMAND ----------

data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC For make predictions and based the analysis , the best columns [status, price, bed, bath ,city , state , zip_code ,house_size

# COMMAND ----------

# select the columns and new dataframe
new_df=data[["status","price","bed","bath","city","state","house_size"]]

# COMMAND ----------

new_df.head()

# COMMAND ----------

new_df.isnull().sum()

# COMMAND ----------

new_df.info()

# COMMAND ----------

# separation the data type columns [ object and numeric ]

cat = []
num = []
for i in new_df.columns:
    if new_df[i].dtypes == 'O':
        cat.append(i)
    else:
        num.append(i)

data_n=new_df[num]   # new dataframe just type numeric
data_c=new_df[cat]  # new dataframe just type object

# COMMAND ----------

col=data_c.columns
col

# COMMAND ----------

label_encoders = {}
categorical_columns = data_c.columns  # I would recommend using columns names here if you're using pandas. If you're using numpy then stick with range(n) instead

for column in categorical_columns:
    label_encoders[column] = LabelEncoder()
    data_c[column] = label_encoders[column].fit_transform(data_c[column])

# COMMAND ----------

 # merge 2 data set 
frames = [data_c, data_n]
  
data1 = pd.concat(frames,axis=1)
data1.head()

# COMMAND ----------

data1.info()

# COMMAND ----------

data1.shape

# COMMAND ----------

#Defined X value and y value , and split the data train
X = data1.drop(columns="price")           
y = data1["price"]    # y = price

# split the data train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)

print("X Train : ", X_train.shape)
print("X Test  : ", X_test.shape)
print("Y Train : ", y_train.shape)
print("Y Test  : ", y_test.shape)


# COMMAND ----------

#Defined object from library Regression 

LR = LinearRegression()
DTR = DecisionTreeRegressor()
RFR = RandomForestRegressor()

# COMMAND ----------

# make for loop for Regression 

li = [LR,DTR,RFR]
d = {}
for i in li:
    i.fit(X_train,y_train)
    ypred = i.predict(X_test)
    print(i,":",r2_score(y_test,ypred)*100)
    d.update({str(i):i.score(X_test,y_test)*100})

# COMMAND ----------

# make graph about Accuracy

plt.figure(figsize=(30, 6))
plt.title("Algorithm vs Accuracy")
plt.xlabel("Algorithm")
plt.ylabel("Accuracy")
plt.plot(d.keys(),d.values(),marker='o',color='red')
plt.show()

# COMMAND ----------

