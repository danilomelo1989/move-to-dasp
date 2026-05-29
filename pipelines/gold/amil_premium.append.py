# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from premium_union_rules import *
from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat, regexp_extract, lpad, regexp_replace, datediff, regexp_replace, sum, countDistinct, coalesce, floor, length, lpad
from pyspark.sql.types import StringType
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def process_and_append_tables(conn, schema, codigo_operadora = '', nome_operadora = ''):
    premio= conn.get_table_from_snowflake("PREMIO_AMIL", 'DATABRICKS_SILVER')
    copay_header= conn.get_table_from_snowflake("PARTICIPACAO_1_AMIL", 'DATABRICKS_SILVER')
    copay_body= conn.get_table_from_snowflake("PARTICIPACAO_2_AMIL", 'DATABRICKS_SILVER')

    # Perform the joins and transformations
    premio = premio.select(
        col("CODIGO_CONTRATO"),
        concat(lpad(col("CODIGO_CONTRATO"), 7, '0'), col("CODIGO_EMPRESA_OPERADORA")).alias("CODIGO_EMPRESA_GRUPO"),
        col("CODIGO_PLANO"),
        col("DESCRICAO_GENERO"),
        col("CODIGO_CARTEIRINHA"),
        to_date(regexp_extract(regexp_replace(col("ETL_SOURCE_ZIP_NAME"), ".zip", ""), r"(\d{6})", 1), "yyyyMM").alias("DATA_REFERENCIA"),
        floor(datediff(to_date(regexp_extract(regexp_replace(col("ETL_SOURCE_ZIP_NAME"), ".zip", ""), r"(\d{6})", 1), "yyyyMM"), col("DATA_NASCIMENTO")) / 365.25).alias("IDADE_BENEFICIARIO").cast("int").cast(StringType()),
        when(col("CODIGO_TIPO_BENEFICIARIO") == '00', lit('T'))
        .otherwise(lit('D')).alias("CODIGO_TIPO_BENEFICIARIO"),
        lit("00").alias("CD_DIVISAO"),
        col("VALOR_PAGO"),
        lit(0).alias("VALOR_ACERTO"),
        lit(0).alias("VALOR_FRANQUIA"),
        lit(0).alias("VALOR_APORTE"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER")            
    )

    copay_header = copay_header.select(
        to_date(col("COMPETENCIA"), "yyyyMM").alias("DATA_COMPETENCIA"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME")
    )

    copay_body= copay_body.select(
        to_date(col("DATA_REFERENCIA"), "yyyyMM").alias("DATA_REFERENCIA"),
        col("CODIGO_CARTEIRINHA"),
        col("CODIGO_CONTRATO").alias("CODIGO_EMPRESA_GRUPO"),
        col("VALOR_COPARTICIPACAO"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME")
    )

    copay = copay_body.join(copay_header,
                                 on=[
                                     "ETL_SOURCE_FILE_NAME",
                                     "ETL_SOURCE_ZIP_NAME",
                                 ], how="left") \
                                     .drop(col("ETL_SOURCE_FILE_NAME"), col("ETL_SOURCE_ZIP_NAME"))
    

    premio_copay = premio.join(copay,
                                 on=[
                                     "CODIGO_CARTEIRINHA",
                                     "CODIGO_EMPRESA_GRUPO",
                                     "DATA_REFERENCIA"
                                 ], how="left") \
                                    .withColumn("CODIGO_OPERADORA", lit(codigo_operadora)) \
                                    .withColumn("NOME_OPERADORA", lit(nome_operadora)) \
                                    .withColumn("DATE_TIME_SILVER_TO_GOLD", current_timestamp())
                                    

    aggregated_premio_copay = premio_copay.groupBy(
    "CODIGO_OPERADORA",
    "CODIGO_CONTRATO",
    "CODIGO_EMPRESA_GRUPO",
    "CODIGO_PLANO",
    "IDADE_BENEFICIARIO",
    "DESCRICAO_GENERO",
    "DATA_REFERENCIA",
    "CODIGO_TIPO_BENEFICIARIO",
    "CD_DIVISAO"
    ).agg(
        sum("VALOR_PAGO").alias("VALOR_PAGO"),
        sum("VALOR_COPARTICIPACAO").alias("VALOR_COPARTICIPACAO"),
        countDistinct("CODIGO_CARTEIRINHA").alias("QUANTIDADE_BENEFICIARIO_ATIVO")
    )

    premio_final = aggregated_premio_copay \
                                .withColumn("CODIGO_OPERADORA", lit(codigo_operadora)) \
                                .withColumn("NOME_OPERADORA", lit(nome_operadora)) \
                                .withColumn("DATE_TIME_SILVER_TO_GOLD", current_timestamp())
                                                                                                 
                    
    append_to_snowflake_table(conn, premio_final, 'TESTE_PREMIO_UNIFICADO', 'GOLD')

CODIGO_OPERADORA = '27'
NOME_OPERADORA = 'AMIL'

conn = SnowflakeConnector()
process_and_append_tables(conn, unified_schema, CODIGO_OPERADORA, NOME_OPERADORA)
