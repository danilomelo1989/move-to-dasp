# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

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
def standardize_servers(table, prestadores):
    distinct_prestadores = prestadores.select('NOME_PRESTADOR_SAS', 'NOVO_NOME_PRESTADOR').distinct()
    
    final_df = table.alias('TABLE').join(
        distinct_prestadores.alias('PRESTADORES'),
        on=F.upper(table['DESCRICAO_PRESTADOR']) == F.upper(distinct_prestadores['NOME_PRESTADOR_SAS']),
        how='left'
    )
    
    table = final_df.select(
        'TABLE.*',
        'PRESTADORES.NOME_PRESTADOR_SAS',
        'PRESTADORES.NOVO_NOME_PRESTADOR'
    )
    return table
    
conn = SnowflakeConnector()
table = conn.get_table_from_snowflake('SINISTRO_ONE_HEALTH', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')
companies = conn.get_table_from_snowflake('EMPRESAS', 'AUXILIAR')
plans = conn.get_table_from_snowflake('PLANOS', 'AUXILIAR')
servers = conn.get_table_from_snowflake('PRESTADORES', 'AUXILIAR')
table = add_datetime_stamp(table,"DATE_TIME_SILVER_TO_GOLD")
table = generate_hospitalization_key(table)
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table, "CODIGO_CARTEIRINHA")
table = generate_complex_treatment_key(table, "CODIGO_CARTEIRINHA")
table = generate_emergency_care_key(table, "CODIGO_CARTEIRINHA")
table = standardize_plans(table, companies, plans)
table = standardize_servers(table, servers)
conn.save_snowflake_table(table, 'ONE_HEALTH_CLASSIFICACAO', 'GOLD', 'Overwrite')
