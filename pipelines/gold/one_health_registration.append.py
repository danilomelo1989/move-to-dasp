# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union
from pyspark.sql.functions import lit, col, when, to_date, concat, regexp_extract, lpad, regexp_replace, sum, lpad
from pyspark.sql.types import StringType
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def create_registration_one_health(conn):
    premio= conn.get_table_from_snowflake("PREMIO_ONE_HEALTH", 'DATABRICKS_SILVER')

    # Perform the joins and transformations
    cadastro = premio.select(
        col("CODIGO_CONTRATO"),
        col("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        concat(lpad(col("CODIGO_CONTRATO"), 7, '0'), col("CODIGO_EMPRESA_OPERADORA")).alias("CODIGO_EMPRESA_GRUPO"),
        col("CODIGO_PLANO"),
        col("DESCRICAO_GENERO"),
        col("CODIGO_CARTEIRINHA").alias("CODIGO_BENEFICIARIO"),
        col("CODIGO_FAMILIA"),
        col("NOME_BENEFICIARIO"),
        to_date(regexp_extract(regexp_replace(col("ETL_SOURCE_ZIP_NAME"), ".zip", ""), r"(\d{6})", 1), "yyyyMM").alias("DATA_REFERENCIA"),
        col("DATA_NASCIMENTO"),
        when(col("CODIGO_TIPO_BENEFICIARIO") == '00', lit('TITULAR'))
        .otherwise(lit('DEPENDENTE')).alias("DESCRICAO_ELEGIBILIDADE"),
        col("CPF").alias("CODIGO_CPF_DEPENDENTE"),                
        lit("00").alias("CD_DIVISAO"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER")            
    )

    cpf_titular = cadastro \
        .filter('DESCRICAO_ELEGIBILIDADE = "TITULAR"') \
        .select(
        col("CODIGO_CONTRATO"),
        col("CODIGO_EMPRESA_GRUPO"),
        col("DATA_REFERENCIA"),
        col("CODIGO_FAMILIA"),
        col("ETL_SOURCE_FILE_NAME"),
        col("CODIGO_CPF_DEPENDENTE").alias("CODIGO_CPF_TITULAR")
    ).distinct()

    cadastro_final = cadastro.join(cpf_titular,
                                 on=[
                                     "CODIGO_CONTRATO",
                                     "CODIGO_EMPRESA_GRUPO",
                                     "DATA_REFERENCIA",
                                     "CODIGO_FAMILIA",
                                     "ETL_SOURCE_FILE_NAME"
                                 ], how="left")
                                     #.drop(col("ETL_SOURCE_FILE_NAME"), col("ETL_SOURCE_ZIP_NAME"))
    return cadastro_final
    
                                    
CODIGO_OPERADORA = '62'
NOME_OPERADORA = 'ONE_HEALTH'
conn = SnowflakeConnector()

registration = create_registration_one_health(conn)
registration = fastview_union.define_carrier_code(registration, CODIGO_OPERADORA, NOME_OPERADORA)
registration = fastview_union.join_company_name(conn, registration)
registration = fastview_union.calculate_age(registration)

table = fastview_union.adapt_layout(registration, fastview_union.REGISTRATION)
conn.save_snowflake_table(table, 'CADASTRO_UNIFICADO', 'DEBUG', 'Append')
