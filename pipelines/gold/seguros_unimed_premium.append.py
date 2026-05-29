# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union 
from premium_union_rules import *
from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat_ws, regexp_extract, lpad, regexp_replace, month, add_months,trunc, sum ,floor

from pyspark.sql.dataframe import  *
#from pyspark.sql.column import *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def get_premium_seguros_unimed(conn):
    premium = conn.get_table_from_snowflake("PREMIO_SEGUROS_UNIMED", 'DATABRICKS_SILVER')
    premium = premium.select(
            col("CODIGO_CONTRATO"),
            col("DATA_REFERENCIA"),
            col("CODIGO_PLANO"),
            col("DESCRICAO_GENERO"),
            col("IDADE_BENEFICIARIO"),
            col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
            when(col("CODIGO_ELEGIBILIDADE") == "T", "TITULAR")
            .when(col("CODIGO_ELEGIBILIDADE") == "D" , "DEPENDENTE")
            .when(col("CODIGO_ELEGIBILIDADE") == "A" , "DEPENDENTE")
            .otherwise("DEMAIS").alias("CODIGO_TIPO_BENEFICIARIO"),
            col("VALOR_PAGO"),
            col("VALOR_ACERTO"),
            col("VALOR_FRANQUIA"),
            col("VALOR_COPARTICIPACAO"),
            col("ETL_SOURCE_FILE_NAME"),
            col("ETL_SOURCE_ZIP_NAME"),
            col("RUN_ID"),
            col("DATE_TIME_LOAD_STG"),
            col("SOURCE_FILE_NAME_LINE_NUMBER"),
            col("DATE_TIME_STG_TO_SILVER")
    )         
    return premium



CODIGO_OPERADORA = '14'
NOME_OPERADORA = 'SEGUROS_UNIMED'
conn = SnowflakeConnector()

premium = get_premium_seguros_unimed(conn)
premium = fastview_union.define_carrier_code(premium, CODIGO_OPERADORA, NOME_OPERADORA)
#TODO: operadora 14 seguros unimed não existe na tabela NONPROD_BRAZILHEALTHANALYTICS_BRADESCO.GOLD.EMPRESAS
premium = fastview_union.join_company_name(conn, premium)
table = fastview_union.adapt_layout(premium, fastview_union.PREMIUM)
conn.save_snowflake_table(table, 'PREMIO_UNIFICADO', 'DEBUG', 'append')
