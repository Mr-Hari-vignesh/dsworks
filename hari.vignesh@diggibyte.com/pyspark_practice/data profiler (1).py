# Databricks notebook source
pip install DataProfiler[full]

# COMMAND ----------

import json
from dataprofiler import Data, Profiler
import dataprofiler as dp
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql import Row

import pandas as pd
my_dataframe = spark.read.format("json").load("abfss://data@elxa2ns0001dev.dfs.core.windows.net/bronze/amazon_vendor_central/amzn1.application-oa2-client.3d246abbe62b4288a287217a0dcdfdb4/MANUFACTURING/A1PA6795UKMFR9/vendor_sales_report_manufacturing/20220401").toPandas()
data = dp.Profiler(my_dataframe)

report  = data.report(report_options={"output_format":"pretty"})
print(json.dumps(report, indent=4))

#print(json.dumps(readable_report, indent=4))