# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install chardet
# MAGIC %pip install types-chardet
# MAGIC %pip install tabula-py
# MAGIC %pip install PyMuPDF tabula-py
# MAGIC %pip install openpyxl
# MAGIC %pip install xlrd

# COMMAND ----------

"""SINISTRO PARANA_CLINICAS"""
dbutils.library.restartPython()
from modules.customers.sas.pipeline import execute_pipeline

execute_pipeline("PARANA_CLINICAS", "2013")

# COMMAND ----------

"""SINISTRO PARANA_CLINICAS"""
dbutils.library.restartPython()
from modules.customers.sas.pipeline import execute_pipeline

execute_pipeline("PARANA_CLINICAS", "HASH")