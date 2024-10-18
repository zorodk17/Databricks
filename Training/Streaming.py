# Databricks notebook source
schema = "id Int, Name String, Gender String, salary Int, Country String, Date String"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sony_dev.bronze.stream

# COMMAND ----------

(spark
 .readStream
 .schema(schema)
 .csv("/Volumes/sony_dev/bronze/stream/",header=True)
    .writeStream
    .option("checkpointLocation", "/FileStore/tables/checkpoint2")
    .trigger(availableNow=True)
    .table("Sony_dev.bronze.stream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sony_dev.bronze.stream
