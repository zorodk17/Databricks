# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
# MAGIC RETURNS STRING
# MAGIC RETURN concat("The ", item_name," is on sale for $", round(item_price * 0.8, 0));

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, sale_announcement(product_id, total_amount) as discount from deepak_databricks.default.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sale_announcement
