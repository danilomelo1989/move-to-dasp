# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union 
# from registration_union_rules import *
from pyspark.sql.functions import lit

CODIGO_OPERADORA = '9'
NOME_OPERADORA = 'SULAMERICA'

conn = SnowflakeConnector()
table = conn.get_snowflake_table("CADASTRO_SULAMERICA", "DATABRICKS_SILVER")
table = fastview_union.define_carrier_code(table, CODIGO_OPERADORA, NOME_OPERADORA)
table = fastview_union.join_company_name(conn, table)
table = fastview_union.adapt_layout(table, fastview_union.REGISTRATION)
conn.save_snowflake_table(table, 'CADASTRO_UNIFICADO', 'DEBUG', 'Overwrite')

# process_and_append_tables(conn, table_names, unified_schema, CODIGO_OPERADORA, NOME_OPERADORA)
