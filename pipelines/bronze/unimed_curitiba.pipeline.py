# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install chardet
# MAGIC %pip install types-chardet
# MAGIC %pip install tabula-py
# MAGIC %pip install PyMuPDF tabula-py
# MAGIC %pip install openpyxl

# COMMAND ----------

from modules.customers.unimed_curitiba.pipeline import execute_pipeline
from modules.customers.unimed_curitiba.pipeline import execute_pipeline_registry
from modules.customers.unimed_curitiba.pipeline import execute_pipeline_claims

file_type = dbutils.widgets.get('file_type')
date = dbutils.widgets.get('date')

standard_pipeline = ["GERENCIAL"]

if file_type in standard_pipeline:
    execute_pipeline(file_type, date)
elif file_type == 'CADASTRO':
    execute_pipeline_registry(file_type, date)
elif file_type == 'SINISTRO':
    execute_pipeline_claims(file_type, date)
else:
    raise ValueError

