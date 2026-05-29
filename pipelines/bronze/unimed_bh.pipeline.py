# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install chardet
# MAGIC %pip install types-chardet
# MAGIC %pip install tabula-py
# MAGIC %pip install PyMuPDF tabula-py
# MAGIC %pip install openpyxl
# MAGIC %pip install xlrd

# COMMAND ----------

from modules.customers.unimed_bh.pipeline import execute_pipeline

file_type = dbutils.widgets.get('file_type')
date = dbutils.widgets.get('date')

print (file_type)
print (date)

standard_pipeline = ["PREMIO", "SINISTRO"]

if file_type in standard_pipeline:
    execute_pipeline(file_type, date)
else:
    raise ValueError
