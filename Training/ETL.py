# Databricks notebook source
# MAGIC %run /Workspace/Users/deepakkumarabd17@gmail.com/Databricks/Training/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales

# COMMAND ----------


df1.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=df1.withColumn("environment",lit("uat"))

# COMMAND ----------

df2.display()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("env","dev")

# COMMAND ----------

v=dbutils.widgets.get("env")
