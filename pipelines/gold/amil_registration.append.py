# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from registration_union_rules import *
from pyspark.sql.functions import lit

conn = SnowflakeConnector()
table_names = [
    "CADASTRO_AMIL"
]

CODIGO_OPERADORA = '27'
NOME_OPERADORA = 'AMIL'

process_and_append_tables(conn, table_names, unified_schema, CODIGO_OPERADORA, NOME_OPERADORA)
