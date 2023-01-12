# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/tf-abfss/bronze/indian_railways

# COMMAND ----------

from pyspark.sql.functions import explode 

# COMMAND ----------

df = spark.read.option("multiline","true").json("/mnt/tf-abfss/bronze/indian_railways/stations.json")
df.printSchema()

# COMMAND ----------



# COMMAND ----------

df0 = df.select(explode("features"))
display(df0)

# COMMAND ----------

df1 = df0.select("col.*")
display(df1)

# COMMAND ----------

df2 = df1.select("geometry.*", "properties.*",explode("coordinates"))
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df3 = df2.select(explode("coordinates"))
display(df3)

# COMMAND ----------

#df2.write.format('json').saveAsTable("/silver/railway_station/station.json")

# COMMAND ----------

#df2.write.format("json").mode('overwrite').saveAsTable("/cntexapure/silver/RL_station/RL_station.json")

# COMMAND ----------

#dbutils.fs.rm("/cntexapure/")

# COMMAND ----------

