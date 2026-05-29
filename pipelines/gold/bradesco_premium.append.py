# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from premium_union_rules import *
from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat_ws, regexp_extract, lpad, regexp_replace
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def process_and_append_tables(conn, schema, codigo_operadora = '', nome_operadora = ''):
    header_1 = conn.get_table_from_snowflake("PREMIO_1_BRADESCO", 'DATABRICKS_SILVER')
    header_2 = conn.get_table_from_snowflake("PREMIO_2_BRADESCO", 'DATABRICKS_SILVER')
    fatura = conn.get_table_from_snowflake("PREMIO_3_BRADESCO", 'DATABRICKS_SILVER')

    header_1 = header_1.select(
        col("CODIGO_CONTRATO"),
        col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        col("DATA_REFERENCIA"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME")
    )

    fatura = fatura.select(
        #col("CODIGO_FAMILIA"),
        col("CODIGO_DEPENDENCIA"),
        col("NOME_BENEFICIARIO"),
        col("CODIGO_GENERO"),
        col("CODIGO_PLANO"),
        col("CODIGO_PAGAMENTO"),
        col("CODIGO_TIPO_PAGAMENTO"),
        col("VALOR_PAGO"),
        col("VALOR_COPARTICIPACAO"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER")        
    )

    # Perform the joins and transformations
    fatura_header = fatura \
        .join(header_1, on=["ETL_SOURCE_FILE_NAME", "ETL_SOURCE_ZIP_NAME"], how="left") 

    transformed_df = fatura_header.select(
        col("CODIGO_CONTRATO"),
        col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        col("NOME_BENEFICIARIO"),
        col("DATA_REFERENCIA"),
        when(col("CODIGO_DEPENDENCIA") == '0', lit('T')).otherwise(lit('D')).alias("CODIGO_TIPO_BENEFICIARIO"),
        col("CODIGO_DEPENDENCIA"),
        col("CODIGO_GENERO"),
        col("CODIGO_PLANO"),
        col("VALOR_COPARTICIPACAO"),
        lit("00").alias("CD_DIVISAO"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER"),
        # Separated columns for each possible TP_VALOR_PAGO value
        when(~col("CODIGO_TIPO_PAGAMENTO").isin(['RS', 'AC', 'AD']), col("VALOR_PAGO")).alias("VALOR_PREMIO"),
        when(col("CODIGO_TIPO_PAGAMENTO") == 'RS', col("VALOR_PAGO")).alias("VALOR_FRANQUIA"),
        when(((col("CODIGO_TIPO_PAGAMENTO") == 'AC') & (col("VALOR_PAGO") >= 0) & (col("NOME_BENEFICIARIO").like('%APORTE%'))) | ((col("CODIGO_TIPO_PAGAMENTO") == 'AC') & (col("VALOR_PAGO") >= 50000)), col("VALOR_PAGO")).alias("VALOR_APORTE"),
        when(col("CODIGO_TIPO_PAGAMENTO").isin(['AD', 'AC']), col("VALOR_PAGO")).alias("VALOR_ACERTO")
    )


    transformed_df = transformed_df.withColumn("DATE_TIME_SILVER_TO_GOLD", current_timestamp()) \
            .withColumn("CODIGO_OPERADORA", lit(codigo_operadora)) \
            .withColumn("NOME_OPERADORA", lit(nome_operadora)) 
    append_to_snowflake_table(conn, fatura_header, 'TESTE_PREMIO_UNIFICADO_SOBRINHO', 'GOLD')

CODIGO_OPERADORA = '1'
NOME_OPERADORA = 'BRADESCO'

conn = SnowflakeConnector()
process_and_append_tables(conn, unified_schema, CODIGO_OPERADORA, NOME_OPERADORA)
