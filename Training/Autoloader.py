# Databricks notebook source
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation","/FileStore/tables/schemaLocation")
.load("/Volumes/sony_dev/bronze/stream/")
.writeStream.option("checkpointLocation","/FileStore/tables/checkpoint/autoloader").table("sony_dev.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sony_dev.bronze.autoloader
