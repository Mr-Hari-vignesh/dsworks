# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/loan_approval_hari/2_rep_eda

# COMMAND ----------

# DBTITLE 1,train data pre-processing
#After analysis, I came to know that categorical data have many null values and so do numerical data have. So, I am going to handle nulls in below code:
data_tr.Gender.fillna('Not Given', inplace=True)
data_tr.Married.fillna('Not Given', inplace=True)
data_tr.Self_Employed.fillna('Not Given', inplace=True)
data_tr.LoanAmount.fillna(data_tr.LoanAmount.mean(),inplace=True)
data_tr.Loan_Amount_Term.fillna(360.0, inplace=True)
data_tr.Credit_History.fillna(1.0, inplace=True)
data_tr.Dependents.fillna(0, inplace=True)

data_tr.isna().sum()

# COMMAND ----------

#Now we also noticed, that there exist non numeric values in "Dependents" column. I am going to handle them like that:
data_tr.Dependents.replace(to_replace='3+', value=4)

# COMMAND ----------

#checking for null values
data_tr.isna().sum()

# COMMAND ----------

#Now, Preparing features and labels (training data) for ML model implementation
feature_tr=pd.get_dummies(data_tr.iloc[:,1:12]) #get dummies used for data manipulation(it converts catogarical data to indicator #varibles((ie:0 or 1))
labels_tr=data_tr["Loan_Status"]
feature_tr.head()

# COMMAND ----------

feature_tr.describe()

# COMMAND ----------

len(feature_tr)

# COMMAND ----------

len(labels_tr)

# COMMAND ----------

labels_tr=data_tr.Loan_Status.map(dict(Y=1,N=0))
labels_tr.shape

# COMMAND ----------

plt.figure(figsize=(10,6))
sns.heatmap(data_tr.corr(), annot=True)