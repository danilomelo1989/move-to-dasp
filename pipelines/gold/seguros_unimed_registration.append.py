# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union 
from premium_union_rules import *
from pyspark.sql.functions import  col
from pyspark.sql.dataframe import  *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def get_registration_seguros_unimed(conn):
    registration = conn.get_table_from_snowflake("PREMIO_SEGUROS_UNIMED", 'DATABRICKS_SILVER')
    registration = registration.select(
            col("CODIGO_CONTRATO"),
            col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
            col("CODIGO_BENEFICIARIO"),
            col("NOME_BENEFICIARIO"),
            col("DATA_NASCIMENTO"),
            col("DESCRICAO_GENERO"),
            col("CODIGO_PLANO"),
            col("DESCRICAO_ELEGIBILIDADE"),
            col("DATA_REFERENCIA"),
            col("ETL_SOURCE_FILE_NAME"),
            col("ETL_SOURCE_ZIP_NAME"),
            col("RUN_ID"),
            col("DATE_TIME_LOAD_STG"),
            #col("SOURCE_FILE_NAME_LINE_NUMBER"), #para que o distinct funcionasse corretamente retiramos esse campo
            col("DATE_TIME_STG_TO_SILVER")
    ).distinct()
    return registration



CODIGO_OPERADORA = '14'
NOME_OPERADORA = 'SEGUROS UNIMED'
conn = SnowflakeConnector()

registration = get_registration_seguros_unimed(conn)
registration = fastview_union.define_carrier_code(registration, CODIGO_OPERADORA, NOME_OPERADORA)
#TODO: operadora 14 seguros unimed não existe na tabela NONPROD_BRAZILHEALTHANALYTICS_BRADESCO.GOLD.EMPRESAS
registration = fastview_union.join_company_name(conn, registration)
table = fastview_union.adapt_layout(registration, fastview_union.REGISTRATION)
conn.save_snowflake_table(table, 'CADASTRO_UNIFICADO', 'DEBUG', 'append')
