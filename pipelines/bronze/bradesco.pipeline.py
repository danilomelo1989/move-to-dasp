# Databricks notebook source
# COMMAND ----------
#MAGIC %pip install snowflake-connector-python
#MAGIC %pip install chardet
#MAGIC %pip install types-chardet
#MAGIC %pip install tabula-py
#MAGIC %pip install PyMuPDF tabula-py
#MAGIC %pip install openpyxl
#MAGIC %pip install xlrd

# COMMAND ----------
from modules.customers.bradesco.pipeline import execute_pipeline
from modules.customers.bradesco.pipeline import execute_pipeline_managerial

file_type = dbutils.widgets.get('file_type')
date = dbutils.widgets.get('date')

standard_pipeline = ["SINISTRO", "PREMIO", "CADASTRO"]

if file_type in standard_pipeline:
    execute_pipeline(file_type, date)
elif file_type == 'GERENCIAL':
    execute_pipeline_managerial(date)
else:
    raise ValueError