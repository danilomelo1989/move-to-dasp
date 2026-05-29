# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install chardet
# MAGIC %pip install types-chardet
# MAGIC %pip install tabula-py
# MAGIC %pip install PyMuPDF tabula-py
# MAGIC %pip install openpyxl

# COMMAND ----------

from modules.customers.unimed_federacaors.pipeline import execute_pipeline

file_type = dbutils.widgets.get('file_type')
date = dbutils.widgets.get('date')

standard_pipeline = ["SINISTRO", "CADASTRO"]

if file_type in standard_pipeline:
    execute_pipeline(file_type, date)
else:
    raise ValueError

