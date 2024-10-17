# Databricks notebook source
# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/deepak_databricks/default/raw/customers.json`
