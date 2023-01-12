# Databricks notebook source
dbutils.fs.ls('/')

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/'))

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/user.csv")
display(df1)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

dbutils.fs.head('FileStore/tables/SQL_Assessment2.csv',25)


# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

name = 'puppy'
dbutils.notebook.exit(name)

# COMMAND ----------

name = 'puppy'
print(name)

# COMMAND ----------

# create mount point
dbutils.fs.mount(
    source = 'wasbs://bronze@assignstorage.blob.core.windows.net/',
    mount_point = '/mnt/assignstorage',
    extra_configs = {'fs.azure.account.key.assignstorage.blob.core.windows.net':'IJq3U0AnCuzYYmMqLmu3IgzbFKYqlK/z6q5n0z4fXXgoGgoMj6cGRmGngjUqCa4eIX5W4UYrsEVi+AStKAF9RA=='})

# COMMAND ----------

dbutils.fs.ls('/mnt/assignstorage') # use mount point to access the data in container

# COMMAND ----------

dbutils.fs.cp('/mnt/assignstorage/Iris (1).csv','/mnt/assignstorage/data/Iris (1).csv')

# COMMAND ----------

df=spark.read.csv('/mnt/assignstorage/Iris (1).csv',header=True)
df.show()

# COMMAND ----------

df1=df.limit(10)
df1.show()

# COMMAND ----------

# unmount the mount point

dbutils.fs.unmount('/mnt/assignstorage')

# COMMAND ----------

# spark.conf.set("fs.azure.account.key.<storage name>.dfs.core.windows.net",
# dbutils.secrets.get(scope="<scope-name>",key="<storage-account-access-key-name>"))

# COMMAND ----------

# MAGIC %run