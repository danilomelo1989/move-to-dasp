# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from premium_union_rules import *
from pyspark.sql.functions import lit, col, expr, when, floor
import pyspark.sql.functions as functions

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def get_sas_premium(carrier):
    table = conn.get_table_from_snowflake(f'{carrier}_TB_PREM', 'SAS_SILVER')
    company = conn.get_table_from_snowflake(f'{carrier}_TB_OP_EMP', 'SAS_SILVER')
    
    table = table.join(
        company,
        ['DT_REFR', 'CD_OPRD', 'CD_EMPR'],
        'LEFT'
    ) \
    .select(
        col("CD_OPRD").alias("CODIGO_OPERADORA"),
        col("NM_OPRD").alias("NOME_OPERADORA"),
        col("CD_EMPR").alias("CODIGO_EMPRESA"),
        col("NM_EMPR").alias("NOME_EMPRESA"),
        col("DT_REFR").alias("DATA_REFERENCIA"),
        col("CD_APLC").alias("CODIGO_CONTRATO"),
        col("CD_EMPR_GRPO").alias("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        col("CD_PLNO").alias("CODIGO_PLANO"),
        col("FL_SEXO").alias("DESCRICAO_GENERO"),
        col("NR_IDDE").alias("IDADE_BENEFICIARIO"),
        col("NM_FAIX_ETRA").alias("FAIXA_ETARIA"),
        col("QTDE_VIDA").alias("QUANTIDADE_BENEFICIARIO_ATIVO"),
        col("NM_CATG_USRO").alias("CODIGO_TIPO_BENEFICIARIO"),
        col("VL_PREM").alias("VALOR_PAGO"),
        col("VL_ACRT").alias("VALOR_ACERTO"),
        col("VL_FRNQ").alias("VALOR_FRANQUIA"),
        col("VL_APRT").alias("VALOR_APORTE"),
        col("VL_CO_PART").alias("VALOR_COPARTICIPACAO") 
    ) \
    .filter(col("DATA_REFERENCIA").between('2022-01-01', '2022-12-31'))
    return table

conn = SnowflakeConnector()
table = get_sas_premium('SULAMERICA')
append_to_snowflake_table(conn, table, 'PREMIO_UNIFICADO', 'GOLD')
