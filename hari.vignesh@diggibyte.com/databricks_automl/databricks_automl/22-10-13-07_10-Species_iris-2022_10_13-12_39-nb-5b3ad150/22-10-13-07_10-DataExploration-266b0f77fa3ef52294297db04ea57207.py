# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC This notebook performs exploratory data analysis on the dataset.
# MAGIC To expand on the analysis, attach this notebook to the **compute_ml** cluster,
# MAGIC edit [the options of pandas-profiling](https://pandas-profiling.github.io/pandas-profiling/docs/master/rtd/pages/advanced_usage.html), and rerun it.
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/1836425877082632/s?orderByKey=metrics.%60val_roc_auc_score%60&orderByAsc=false)
# MAGIC - Navigate to the parent notebook [here](#notebook/1836425877082633) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC 
# MAGIC Runtime Version: _9.1.x-cpu-ml-scala2.12_

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd
import databricks.automl_runtime

from mlflow.tracking import MlflowClient

# Download input data from mlflow into a pandas DataFrame
# create temp directory to download data
temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], str(uuid.uuid4())[:8])
os.makedirs(temp_dir)

# download the artifact and read it
client = MlflowClient()
training_data_path = client.download_artifacts("8b0881958faf4ddcbfcb39b4a06866fc", "data", temp_dir)
df = pd.read_parquet(os.path.join(training_data_path, "training_data"))

# delete the temp data
shutil.rmtree(temp_dir)

target_col = "Species"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Results

# COMMAND ----------

from pandas_profiling import ProfileReport
df_profile = ProfileReport(df, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)