# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from registration_union_rules import *
from pyspark.sql.functions import lit, col, coalesce
import pyspark.sql.functions as functions

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def get_sas_premium(carrier):
    table = conn.get_table_from_snowflake(f'{carrier}_TB_PREM_DETL', 'SAS_SILVER')
    users = conn.get_table_from_snowflake(f'{carrier}_TB_USROP_HASH', 'SAS_SILVER')
    company = conn.get_table_from_snowflake(f'{carrier}_TB_OP_EMP', 'SAS_SILVER')
    
    table = table.join(
        company,
        ['DT_REFR', 'CD_OPRD', 'CD_EMPR'],
        'LEFT'
    ) \
    .join(
        users,
        ['USRO_HASH'],
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
        coalesce('CD_DPND', 'CD_TTLR').alias('CODIGO_BENEFICIARIO'),
        coalesce('NM_DPND', 'NM_TTLR').alias('NOME_BENEFICIARIO'),
        col('DT_NSCM').alias('DATA_NASCIMENTO'),
        col('DT_ADMS').alias('DATA_INCLUSAO_PLANO'),
        col('DT_EXCL').alias('DATA_EXCLUSAO_PLANO'),
        col('NM_CATG_USRO').alias('DESCRICAO_ELIGIBILIDADE')

    ) \
    .filter(col("DATA_REFERENCIA").between('2022-01-01', '2022-12-31'))
    return table

conn = SnowflakeConnector()
table = get_sas_premium('SULAMERICA')
append_to_snowflake_table(conn, table, 'CADASTRO_UNIFICADO', 'GOLD')
