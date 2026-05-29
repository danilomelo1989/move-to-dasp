# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *

def generate_hospitalization_key(table):
    hospitalization_key = table \
        .filter('CODIGO_AUTORIZACAO != "nan"') \
        .groupBy('CODIGO_AUTORIZACAO') \
        .agg(
            F.min('DATA_PROCEDIMENTO').alias('MIN_PROCEDIMENTO'),
            F.max('DATA_PROCEDIMENTO').alias('MAX_PROCEDIMENTO'),
            F.max('DATA_ALTA_PROCEDIMENTO').alias('MAX_ALTA'),
            F.sum('VALOR_PAGO').alias('CUSTO_TOTAL_INTERNACAO')
        ) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500") \
        .select(
            "CODIGO_AUTORIZACAO",
            F.col("CODIGO_AUTORIZACAO").alias('CHAVE_INTERNACAO_AON'),
            'CUSTO_TOTAL_INTERNACAO',
            F.col('MIN_PROCEDIMENTO').alias('INICIO_INTERNACAO'),
            F.when(
                F.col('MAX_ALTA') != 'nan', 
                F.datediff(F.col('MAX_ALTA'), F.col('MIN_PROCEDIMENTO'))
                ).otherwise(
                    F.datediff(F.col('MAX_PROCEDIMENTO'), F.col('MIN_PROCEDIMENTO'))
                ).alias('QUANTIDADE_DIAS_INTERNADOS'),
            F.lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    table = table.join(
            hospitalization_key,
            'CODIGO_AUTORIZACAO',
            'LEFT',
        )

    return table

conn = SnowflakeConnector()
table = conn.get_table_from_snowflake('SINISTRO_AMIL', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')

table = generate_hospitalization_key(table)
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table, "CODIGO_CARTEIRINHA")
table = generate_complex_treatment_key(table, "CODIGO_CARTEIRINHA")
table = generate_emergency_care_key(table, "CODIGO_CARTEIRINHA")

conn.save_snowflake_table(table, 'AMIL_CLASSIFICACAO', 'GOLD', 'Overwrite')
