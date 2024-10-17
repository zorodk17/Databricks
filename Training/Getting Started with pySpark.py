# Databricks notebook source
print('hello')

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Core,
# MAGIC RDD DataFrame
# MAGIC
# MAGIC DataFrame: Structured API

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
df=spark.createDataFrame(data)
df.display()

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema=["id","name","age"]
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
#schema=["id int","name string","age int"]
schema="id int, name string,age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions
# MAGIC col
# MAGIC
# MAGIC # DataFrame Functions
# MAGIC .select
# MAGIC .alias
# MAGIC .withColumnRenamed
# MAGIC .withColumnsenamed
# MAGIC .withColumn
# MAGIC

# COMMAND ----------

df.select("*")

# COMMAND ----------

df1= df.select("id","name")

# COMMAND ----------

df1.display()

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df2=df.select(col("id").alias("emp_id"))
df2.display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("Current_Date",current_date()).display()

# COMMAND ----------

df.withColumn("")

# COMMAND ----------


