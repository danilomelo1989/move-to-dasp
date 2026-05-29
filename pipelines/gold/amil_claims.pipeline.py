# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *
from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat_ws, regexp_extract, lpad, regexp_replace, date_trunc, min, max, sum, datediff

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def generate_hospitalization_key(table):
    hospitalization_key = table \
        .filter('CODIGO_AUTORIZACAO != "nan"') \
        .groupBy('CODIGO_AUTORIZACAO') \
        .agg(
            min('DATA_PROCEDIMENTO').alias('MIN_PROCEDIMENTO'),
            max('DATA_PROCEDIMENTO').alias('MAX_PROCEDIMENTO'),
            max('DATA_ALTA_PROCEDIMENTO').alias('MAX_ALTA'),
            sum('VALOR_PAGO').alias('CUSTO_TOTAL_INTERNACAO')
        ) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500") \
        .select(
            "CODIGO_AUTORIZACAO",
            col("CODIGO_AUTORIZACAO").alias('CHAVE_INTERNACAO_AON'),
            'CUSTO_TOTAL_INTERNACAO',
            col('MIN_PROCEDIMENTO').alias('INICIO_INTERNACAO'),
            when(
                col('MAX_ALTA') != 'nan', 
                datediff(col('MAX_ALTA'), col('MIN_PROCEDIMENTO'))
                ).otherwise(
                    datediff(col('MAX_PROCEDIMENTO'), col('MIN_PROCEDIMENTO'))
                ).alias('QUANTIDADE_DIAS_INTERNADOS'),
            lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    table = table.join(
            hospitalization_key,
            'CODIGO_AUTORIZACAO',
            'LEFT',
        )

    return table


def process_and_append_tables(df, schema, codigo_operadora = '', nome_operadora = ''):
    # Perform the joins and transformations
    df = df.select(
        col("CODIGO_CONTRATO"),
        col("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        col("DESCRICAO_GENERO"),
        col("CODIGO_PROCEDIMENTO"),
        col("DESCRICAO_PROCEDIMENTO"),
        col("TIPO_ATENDIMENTO"),
        col("DATA_PROCEDIMENTO"),
        col("DATA_REFERENCIA"),
        col("CODIGO_PRESTADOR"),
        col("VALOR_PAGO"),
        col("VALOR_COPARTICIPACAO"),
        col("QUANTIDADE_PROCEDIMENTO"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER")  
            
    )
    
    df = df.withColumn("CODIGO_OPERADORA", lit(codigo_operadora)) \
            .withColumn("NOME_OPERADORA", lit(nome_operadora)) \
            .withColumn("DATE_TIME_SILVER_TO_GOLD", current_timestamp())
                                                                                                 
                    
    append_to_snowflake_table(conn, df, 'TESTE_SINISTRO_UNIFICADO', 'GOLD')

conn = SnowflakeConnector()
table = conn.get_table_from_snowflake('SINISTRO_AMIL', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')

table = generate_hospitalization_key(table)
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table, "CODIGO_CARTEIRINHA")
table = table.withColumn("CODIGO_GRUPO_ECONOMICO_OPERADORA", lit(""))
table = generate_complex_treatment_key(table, "CODIGO_CARTEIRINHA")
table = generate_emergency_care_key(table, "CODIGO_CARTEIRINHA")

CODIGO_OPERADORA = '27'
NOME_OPERADORA = 'AMIL'

process_and_append_tables(table, unified_schema, CODIGO_OPERADORA, NOME_OPERADORA)
