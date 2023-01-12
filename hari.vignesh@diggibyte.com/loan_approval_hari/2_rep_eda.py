# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/loan_approval_hari/1_rep_etl

# COMMAND ----------

# importing the libraries for visualization
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# DBTITLE 1,train data eda
data_tr.head()

# COMMAND ----------

data_tr.isna().sum()

# COMMAND ----------

print(data_tr["Gender"].value_counts())
print(data_tr["Married"].value_counts())
print(data_tr["Dependents"].value_counts())
print(data_tr["Education"].value_counts())
print(data_tr["Self_Employed"].value_counts())
print(data_tr["Credit_History"].value_counts())
print(data_tr.Loan_Status.value_counts())

# COMMAND ----------

#Now, I am going to analyse numerical data
data_tr.describe()

# COMMAND ----------

data_tr.corr()

# COMMAND ----------

data_tr.cov()

# COMMAND ----------

# loan amount histogram plot
sns.displot(data= data_tr, x='LoanAmount')

# Dependents histogram plot
sns.displot(data = data_tr, x='Loan_Amount_Term')

# COMMAND ----------

#After analysis, I came to know that categorical data have many null values and so do numerical data have
print(data_tr.Dependents.value_counts())
print(data_tr.Dependents.isna().sum())

# COMMAND ----------

import matplotlib.pyplot as plt
%matplotlib inline

# COMMAND ----------

data_tr['ApplicantIncome'].hist(bins=50)

# COMMAND ----------

plt.figure(figsize= (12,6))
plt.title("Barplot between education and loanamount with loan status")
sns.barplot(data=data_tr,y='LoanAmount',x='Education', hue='Loan_Status')

# COMMAND ----------

plt.figure(figsize=(10,6))
plt.title("Barplot between education and loanamount with Gender")
sns.barplot(data=data_tr,y='LoanAmount',x='Education', hue='Gender')

# COMMAND ----------

sns.displot(data=data_tr, x='Education')

# COMMAND ----------

sns.displot(data=data_tr, x='Property_Area')

# COMMAND ----------

plt.figure(figsize=(15,8))
plt.title("Barplot between Property_Area and loanamount with Gender")
sns.barplot(data=data_tr,y='LoanAmount',x='Property_Area', hue='Gender')
plt.show()

# COMMAND ----------

sns.heatmap(data_tr.corr(), annot=True)

# COMMAND ----------

