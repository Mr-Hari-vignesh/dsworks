# Databricks notebook source
# MAGIC %run /Users/hari.vignesh@diggibyte.com/loan_approval_hari/3_rep_preprocessing

# COMMAND ----------

# enabling the mlflow autolog
import mlflow 
import mlflow.sklearn
mlflow.sklearn.autolog()

# COMMAND ----------

from sklearn.model_selection import train_test_split
#from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
#%matplotlib inline

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(feature_tr, labels_tr,train_size=0.75, random_state =3)

# COMMAND ----------

print(X_train.shape)
print(y_train.shape)
print(X_test.shape)
print(y_test.shape)

# COMMAND ----------

from sklearn.metrics import accuracy_score, confusion_matrix

# COMMAND ----------

with mlflow.start_run(run_name = 'log_reg'):
    # get mlflow run Id
    run_id = mlflow.active_run().info.run_id
    
    # Logistic Regression Model
    from sklearn.linear_model import LogisticRegression
    log_reg = LogisticRegression()
    log_reg.fit(X_train, y_train)
    
    # predictions
    y_test_pred=log_reg.predict(X_test)
    accuracy = accuracy_score(y_test, y_test_pred)
    print("accuracy score is : ",accuracy)
    
    # logging the metric
    mlflow.log_metric("accuracy", accuracy)

# COMMAND ----------

# DBTITLE 1,Decision tree algorithm
#DECISION TREE CLASSIFIER
from sklearn.tree import DecisionTreeClassifier
dec_tr = DecisionTreeClassifier()
dec_tr.fit(X_train, y_train)

# COMMAND ----------

from sklearn.metrics import accuracy_score
X_train_prediction = dec_tr.predict(X_train)
training_data_accuracy = accuracy_score(y_train,X_train_prediction)

# COMMAND ----------

print("Accuracy score of the training data: ",training_data_accuracy)

# COMMAND ----------

mlflow.log_metric("DT train_data_accuracy", training_data_accuracy)

# COMMAND ----------

dt = DecisionTreeClassifier(max_depth=3, min_samples_leaf=10, random_state=10 )
dt.fit(X_test, y_test)

y_pred = dt.predict(X_test)

# COMMAND ----------

y_test

# COMMAND ----------

# enabling the mlflow autolog
mlflow.sklearn.autolog()

# COMMAND ----------

from sklearn.metrics import accuracy_score, recall_score, roc_auc_score, confusion_matrix
acc_dt=accuracy_score(y_test,y_pred) * 100
print("\nAccuracy score: %f" %(accuracy_score(y_test,y_pred) * 100))
print("Recall score : %f" %(recall_score(y_test, y_pred) * 100))
print("ROC score : %f\n" %(roc_auc_score(y_test, y_pred) * 100))
print(confusion_matrix(y_test, y_pred)) 


# COMMAND ----------

 mlflow.end_run()

# COMMAND ----------

with mlflow.start_run(run_name = 'dt_cls'):
    # get mlflow run Id
    run_id = mlflow.active_run().info.run_id
    
    # Logistic Regression Model
    from sklearn.tree import DecisionTreeClassifier
    dt_classifier = DecisionTreeClassifier()
    dt_classifier.fit(X_train, y_train)
    
    # predictions
    y_test_pred=dt_classifier.predict(X_test)
    accuracy = accuracy_score(y_test, y_test_pred)
    
    # logging the metric
    mlflow.log_metric("accuracy", accuracy)

# COMMAND ----------

