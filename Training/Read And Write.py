# Databricks notebook source
# DBTITLE 1,Read from csv file
#df= spark.read.csv("path")
df = spark.read.csv("/Volumes/deepak_databricks/default/raw/sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

# DBTITLE 1,Read from json
df1 = spark.read.json("/Volumes/deepak_databricks/default/raw/products.json")
df1.display()

# COMMAND ----------

# DBTITLE 1,Write to Table
df.write.mode("overwrite").saveAsTable("sales")
df1.write.mode("overwrite").saveAsTable("products")

# COMMAND ----------

df1.display()
