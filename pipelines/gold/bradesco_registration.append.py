# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from registration_union_rules import *
from pyspark.sql.functions import lit, col, when

def process_and_append_tables(conn, schema):
    titular_df = conn.get_table_from_snowflake("CADASTRO_2_BRADESCO", 'DATABRICKS_SILVER')
    nome_mae_df = conn.get_table_from_snowflake("CADASTRO_5_BRADESCO", 'DATABRICKS_SILVER')
    apolice_df = conn.get_table_from_snowflake("CADASTRO_1_BRADESCO", 'DATABRICKS_SILVER')
    dependente_df = conn.get_table_from_snowflake("CADASTRO_3_BRADESCO", 'DATABRICKS_SILVER')

    # Perform the joins and transformations
    cadastro_titular = titular_df.alias("TITULAR") \
        .join(nome_mae_df.alias("NOME_MAE"), on=["CODIGO_FAMILIA", "ETL_SOURCE_FILE_NAME", "ETL_SOURCE_ZIP_NAME"], how="left") \
        .join(apolice_df.alias("APOLICE"), on=["ETL_SOURCE_FILE_NAME", "ETL_SOURCE_ZIP_NAME"], how="left")

    cadastro_titular = cadastro_titular.select(
        lit(1).alias("CODIGO_OPERADORA"),
        lit("BRADESCO").alias("NOME_OPERADORA"),
        col("TITULAR.TIPO_REGISTRO").alias("CODIGO_PARENTESCO"),
        when(col("TITULAR.TIPO_REGISTRO") == 2, "TITULAR")
        .when(col("TITULAR.TIPO_REGISTRO") == 3, "DEPENDENTE")
        .otherwise("DEMAIS").alias("DESCRICAO_ELEGIBILIDADE"),
        col("TITULAR.CODIGO_EMPRESA_OPERADORA").alias("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        col("TITULAR.CODIGO_FAMILIA"),
        col("TITULAR.NOME_BENEFICIARIO"),
        col("TITULAR.CODIGO_MATRICULA"),
        col("TITULAR.DESCRICAO_GENERO"),
        col("TITULAR.DESCRICAO_ESTADO_CIVIL"),
        col("TITULAR.CPF").alias("CODIGO_CPF_DEPENDENTE"),
        col("TITULAR.CPF").alias("CODIGO_CPF_TITULAR"),
        col("TITULAR.NOME_CARGO"),
        col("TITULAR.CODIGO_PLANO"),
        col("TITULAR.CODIGO_MATRICULA_ESPECIAL"),
        col("TITULAR.DATA_NASCIMENTO"),
        col("TITULAR.DATA_ADMISSAO"),
        col("TITULAR.DATA_INCLUSAO_PLANO"),
        col("TITULAR.DATA_REATIVACAO_PLANO"),
        col("TITULAR.REGIAO"),
        col("TITULAR.DATA_EXCLUSAO_PLANO"),
        col("NOME_MAE.NOME_MAE"),
        col("NOME_MAE.CODIGO_CARTEIRINHA").alias("CODIGO_BENEFICIARIO"),
        col("APOLICE.CODIGO_CONTRATO"),
        col("APOLICE.DATA_REFERENCIA"),
        col("APOLICE.DATA_PROCESSAMENTO"),
        col("TITULAR.ETL_SOURCE_FILE_NAME"),
        col("TITULAR.ETL_SOURCE_ZIP_NAME"),
        col("TITULAR.RUN_ID"),
        col("TITULAR.DATE_TIME_LOAD_STG"),
        col("TITULAR.SOURCE_FILE_NAME_LINE_NUMBER"),
        col("TITULAR.DATE_TIME_STG_TO_SILVER")
    )

    cadastro_dependente = dependente_df.alias("DEPENDENTE") \
        .join(titular_df.alias("TITULAR"), on=["CODIGO_FAMILIA", "ETL_SOURCE_FILE_NAME", "ETL_SOURCE_ZIP_NAME"], how="left") \
        .join(apolice_df.alias("APOLICE"), on=["ETL_SOURCE_FILE_NAME", "ETL_SOURCE_ZIP_NAME"], how="left")

    cadastro_dependente = cadastro_dependente.select(
        lit(1).alias("CODIGO_OPERADORA"),
        lit("BRADESCO").alias("NOME_OPERADORA"),
        col("DEPENDENTE.TIPO_REGISTRO").alias("CODIGO_PARENTESCO"),
        when(col("DEPENDENTE.TIPO_REGISTRO") == 2, "TITULAR")
        .when(col("DEPENDENTE.TIPO_REGISTRO") == 3, "DEPENDENTE")
        .otherwise("DEMAIS").alias("DESCRICAO_ELEGIBILIDADE"),
        col("DEPENDENTE.CODIGO_EMPRESA_OPERADORA").alias("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        col("DEPENDENTE.CODIGO_FAMILIA"),
        col("DEPENDENTE.NOME_BENEFICIARIO"),
        col("TITULAR.CODIGO_MATRICULA"),
        col("DEPENDENTE.DESCRICAO_GENERO"),
        col("DEPENDENTE.CODIGO_ESTADO_CIVIL").alias("DESCRICAO_ESTADO_CIVIL"),
        col("DEPENDENTE.CPF").alias("CODIGO_CPF_DEPENDENTE"),
        col("TITULAR.CPF").alias("CODIGO_CPF_TITULAR"),
        lit("").alias("NOME_CARGO"),
        col("TITULAR.CODIGO_PLANO"),
        col("TITULAR.CODIGO_MATRICULA_ESPECIAL"),
        col("DEPENDENTE.DATA_NASCIMENTO"),
        col("TITULAR.DATA_ADMISSAO"),
        col("DEPENDENTE.DATA_INCLUSAO_PLANO"),
        col("DEPENDENTE.DATA_REATIVACAO_PLANO"),
        col("TITULAR.REGIAO"),
        col("DEPENDENTE.DATA_EXCLUSAO_PLANO"),
        col("DEPENDENTE.NOME_MAE"),
        col("DEPENDENTE.CODIGO_CARTEIRINHA").alias("CODIGO_BENEFICIARIO"),
        col("APOLICE.CODIGO_CONTRATO"),
        col("APOLICE.DATA_REFERENCIA"),
        col("APOLICE.DATA_PROCESSAMENTO"),
        col("DEPENDENTE.ETL_SOURCE_FILE_NAME"),
        col("DEPENDENTE.ETL_SOURCE_ZIP_NAME"),
        col("DEPENDENTE.RUN_ID"),
        col("DEPENDENTE.DATE_TIME_LOAD_STG"),
        col("DEPENDENTE.SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DEPENDENTE.DATE_TIME_STG_TO_SILVER")
    )

    df = cadastro_titular.union(cadastro_dependente)
    df = df.withColumn("DATE_TIME_SILVER_TO_GOLD", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
    append_to_snowflake_table(conn, df, 'TESTE_CADASTRO_UNIFICADO', 'GOLD')

conn = SnowflakeConnector()
process_and_append_tables(conn, unified_schema)
