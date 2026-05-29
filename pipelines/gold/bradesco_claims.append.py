# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *
from pyspark.sql.functions import row_number, min, max, mean, datediff, col, lag, when, sum, array_contains, coalesce, lit, first, last, concat
from pyspark.sql.window import Window

def set_claims_table():
    conn = SnowflakeConnector()
    header = conn.get_table_from_snowflake('sinistro_h_bradesco', 'databricks_silver')
    middle = conn.get_table_from_snowflake('sinistro_m_bradesco', 'databricks_silver')
    service_description = conn.get_table_from_snowflake('BRADESCO_DESCRICAO_SERVICO', 'auxiliar')
    
    header = header \
        .select(
            "CODIGO_CONTRATO",
            "DATA_REFERENCIA",
            "ETL_SOURCE_FILE_NAME"
        )
    
    table = middle \
        .join(
            header,
            'ETL_SOURCE_FILE_NAME',
            'LEFT'
        )
    
    table = table \
        .join(
            service_description,
            'CODIGO_PROCEDIMENTO',
            'LEFT'
        )
    
    return table

def get_necessary_columns(table):
    table = table \
        .select(
            "CODIGO_CONTRATO",
            col("codigo_empresa_operadora") \
                .alias("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
            "DATA_PROCEDIMENTO",
            concat("CODIGO_FAMILIA", "CODIGO_DEPENDENCIA") \
                .alias("CODIGO_BENEFICIARIO"),
            when(
                    col("CODIGO_GENERO") == "1", lit("M")
                ) \
                .otherwise(lit("F")).alias("DESCRICAO_GENERO"),
            #col("IDADE_BENEFICIARIO"),
            when(
                    col("CODIGO_DEPENDENCIA") == "00", "TITULAR"
                ) \
                .otherwise(lit("DEPENDENTE")),
            
            col("CODIGO_PROCEDIMENTO"),
            col("DESCRICAO_PROCEDIMENTO"),
            col("VALOR_PAGO"),
            col("QUANTIDADE_PROCEDIMENTO"),
            col("CODIGO_TIPO_SINISTRO"),
            col("CODIGO_SINISTRO"),
            col("CODIGO_GRUPO_ECONOMICO_OPERADORA") \
                .alias("CODIGO_EMPRESA_OPERADORA"),
            col("DATA_REFERENCIA"),
            col("CODIGO_PRESTADOR"),
            col("NOME_PRESTADOR")
        )
        
    return table

@execute_itermediate_steps(False)
def generate_hospitalization_key(table):
    hospitalization_key = table.filter('''
            CODIGO_TIPO_SINISTRO = 3 OR CODIGO_TIPO_SINISTRO = 4    
        ''') \
        .groupBy("CODIGO_SINISTRO") \
        .agg(
            min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            sum("VALOR_PAGO").alias("CUSTO_TOTAL_INTERNACAO")
        ) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500") \
        .select(
            "CODIGO_SINISTRO",
            col("CODIGO_SINISTRO").alias('CHAVE_INTERNACAO_AON'),
            "INICIO_INTERNACAO", 
            "CUSTO_TOTAL_INTERNACAO",
            lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    table = table.join(
            hospitalization_key,
            'CODIGO_SINISTRO',
            'LEFT',
        )

    return table


table = set_claims_table()
table = get_necessary_columns(table)
table = generate_hospitalization_key(table)

conn = SnowflakeConnector()
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table)
table = generate_complex_treatment_key(table)
table = generate_emergency_care_key(table)

CODIGO_OPERADORA = '1'
NOME_OPERADORA = 'BRADESCO'

process_and_append_tables(conn, table, unified_schema, CODIGO_OPERADORA, NOME_OPERADORA)
