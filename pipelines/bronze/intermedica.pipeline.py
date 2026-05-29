# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install chardet
# MAGIC %pip install types-chardet
# MAGIC %pip install tabula-py
# MAGIC %pip install PyMuPDF tabula-py
# MAGIC %pip install openpyxl
# MAGIC %pip install xlrd

# COMMAND ----------

from modules.customers.intermedica.pipeline import execute_pipeline_claims, execute_pipeline_registration

file_type = dbutils.widgets.get('file_type')
date = dbutils.widgets.get('date')

print (file_type)
print (date)

if file_type == 'SINISTRO':
    execute_pipeline_claims(file_type, date)
elif file_type == 'CADASTRO':
    execute_pipeline_registration(file_type, date)
else:
    raise ValueError
