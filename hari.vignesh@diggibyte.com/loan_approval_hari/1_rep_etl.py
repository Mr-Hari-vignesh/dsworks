# Databricks notebook source
pip install mlflow

# COMMAND ----------

# Import all required libraries for reading data, analysing and visualizing data

import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/data/ds/loan_approval_hari

# COMMAND ----------

data_train= spark.read.option('header',True).csv('/mnt/tf-abfss/data/ds/loan_approval_hari/train_ctrUa4K.csv')
display(data_train)

# COMMAND ----------

data_test= spark.read.option('header',True).csv('/mnt/tf-abfss/data/ds/loan_approval_hari/test_lAUu6dG.csv')
display(data_test)

# COMMAND ----------

# DBTITLE 1,train data etl
data_tr = data_train.toPandas()

# COMMAND ----------

data_tr.head()

# COMMAND ----------

data_tr.dtypes

# COMMAND ----------

data_tr.columns

# COMMAND ----------

data_tr.head()

# COMMAND ----------

data_tr[['CoapplicantIncome','Loan_Amount_Term','ApplicantIncome','LoanAmount']] = data_tr[['CoapplicantIncome','Loan_Amount_Term','ApplicantIncome','LoanAmount']].apply(pd.to_numeric)

# COMMAND ----------

data_tr.dtypes

# COMMAND ----------

# DBTITLE 1,test data etl
data_ts = data_test.toPandas()

# COMMAND ----------

data_ts.head()

# COMMAND ----------

data_ts[['CoapplicantIncome','Loan_Amount_Term','ApplicantIncome','LoanAmount']] = data_ts[['CoapplicantIncome','Loan_Amount_Term','ApplicantIncome','LoanAmount']].apply(pd.to_numeric)

# COMMAND ----------

data_tr.dtypes

# COMMAND ----------

