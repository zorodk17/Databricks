# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists deepak_databricks.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table deepak_databricks.gold.orders_weekofyear as 
# MAGIC select ingestion_date, count(*) as count from deepak_databricks.default.sales group by ingestion_date
