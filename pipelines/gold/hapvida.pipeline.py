# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *
from pyspark.sql.functions import row_number, min, max, mean, datediff, col, lag, when, sum, array_contains, coalesce, lit, first, last, concat
from pyspark.sql.window import Window

def generate_hospitalization_key(table):

    window_spec = Window.partitionBy("CODIGO_BENEFICIARIO").orderBy("DATA_PROCEDIMENTO")
    window_spec2 = Window.partitionBy("CODIGO_BENEFICIARIO").orderBy("DATA_PROCEDIMENTO").rowsBetween(Window.unboundedPreceding, 0)

    hospitalization_key_A = table \
        .filter((col('DESCRICAO_TIPO_SINISTRO') == "INTERNACAO CLINICA") | (col('DESCRICAO_TIPO_SINISTRO') == "INTERNACAO CIRURGICA"))\
        .orderBy("CODIGO_BENEFICIARIO", "DATA_PROCEDIMENTO") \
        .withColumn("lagged_date_diff", datediff(col("DATA_PROCEDIMENTO"), lag("DATA_PROCEDIMENTO", 1).over(window_spec))) \
        .withColumn("start_marker", when(col("lagged_date_diff").isNull() | (col("lagged_date_diff") > 1), 1).otherwise(0)) \
        .withColumn("CHAVE_INTERNACAO_AON", F.sum("start_marker").over(window_spec2)) \
        .groupBy("CODIGO_BENEFICIARIO", "CHAVE_INTERNACAO_AON") \
        .agg(
            F.sum("VALOR_PAGO").alias("CUSTO_TOTAL_INTERNACAO"),
            F.min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            F.max("DATA_PROCEDIMENTO").alias("FINAL_INTERNACAO")
        ) 

    hospitalization_key_B = table \
        .filter((col('DESCRICAO_TIPO_SINISTRO') == "INTERNACAO CLINICA") | (col('DESCRICAO_TIPO_SINISTRO') == "INTERNACAO CIRURGICA"))\
        .orderBy("CODIGO_BENEFICIARIO", "DATA_PROCEDIMENTO") \
        .withColumn("lagged_date_diff", datediff(col("DATA_PROCEDIMENTO"), lag("DATA_PROCEDIMENTO", 1).over(window_spec))) \
        .withColumn("start_marker", when(col("lagged_date_diff").isNull() | (col("lagged_date_diff") > 1), 1).otherwise(0)) \
        .withColumn("CHAVE_INTERNACAO_AON", F.sum("start_marker").over(window_spec2)) \

    hospitalization_key = hospitalization_key_B.join(
            hospitalization_key_A,
            (hospitalization_key_B["CHAVE_INTERNACAO_AON"] == hospitalization_key_A["CHAVE_INTERNACAO_AON"]) & (hospitalization_key_B["CODIGO_BENEFICIARIO"] == hospitalization_key_A["CODIGO_BENEFICIARIO"]),
            'LEFT'
        ) \
        .select(
            hospitalization_key_B["CHAVE_INTERNACAO_AON"],
            hospitalization_key_B["DATA_PROCEDIMENTO"],
            hospitalization_key_B["CODIGO_BENEFICIARIO"],
            hospitalization_key_B["DESCRICAO_TIPO_SINISTRO"],
            hospitalization_key_A["CUSTO_TOTAL_INTERNACAO"],
            hospitalization_key_A["INICIO_INTERNACAO"],
            hospitalization_key_A["FINAL_INTERNACAO"],
            F.lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    hospitalization_key = hospitalization_key \
        .groupBy("CODIGO_BENEFICIARIO", "DATA_PROCEDIMENTO") \
        .agg(
            F.mean("CHAVE_INTERNACAO_AON").alias("CHAVE_INTERNACAO_AON"),
            F.mean("CUSTO_TOTAL_INTERNACAO").alias("CUSTO_TOTAL_INTERNACAO"),
            F.max("INICIO_INTERNACAO").alias("INICIO_INTERNACAO"),
            F.max("FINAL_INTERNACAO").alias("FINAL_INTERNACAO"),
            F.max("NOME_EVENTO").alias("NOME_EVENTO"),
            F.max("DESCRICAO_TIPO_SINISTRO").alias("DESCRICAO_TIPO_SINISTRO")
        ) \
        .withColumn("CHAVE_INTERNACAO_AON", concat("CODIGO_BENEFICIARIO", "CHAVE_INTERNACAO_AON")) \
        .withColumn("PERIODO_INTERNACAO", datediff(col("FINAL_INTERNACAO"), col("INICIO_INTERNACAO"))+1) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500")  

    table = table.join(
            hospitalization_key,
            (table["CODIGO_BENEFICIARIO"] == hospitalization_key["CODIGO_BENEFICIARIO"]) & (table["DATA_PROCEDIMENTO"] == hospitalization_key["DATA_PROCEDIMENTO"]) &  (table["DESCRICAO_TIPO_SINISTRO"] == hospitalization_key["DESCRICAO_TIPO_SINISTRO"]) ,
            'LEFT'
        ) \
        .select(
            table["*"],
            hospitalization_key["CHAVE_INTERNACAO_AON"],
            hospitalization_key["INICIO_INTERNACAO"],
            hospitalization_key["FINAL_INTERNACAO"],
            hospitalization_key["PERIODO_INTERNACAO"],
            hospitalization_key["CUSTO_TOTAL_INTERNACAO"],
            hospitalization_key["NOME_EVENTO"]
        ) 
        .withColumn("CODIGO_AUTORIZACAO", col("CHAVE_INTERNACAO_AON"))
        

    return table

def standardize_plans(table, companies, plan):
    joined_df = table.alias('TABLE').join(
        companies.alias('EMPRESAS'),
        on=col('TABLE.CODIGO_CONTRATO').substr(1, 5) == col('EMPRESAS.CODIGO_APOLICE'),
        how='left'
    )
    
    final_df = joined_df.join(
        plan.alias('PLANOS'),
        on=[
            joined_df['CODIGO_PLANO'] == plan['CD_PLNO'],
            joined_df['EMPRESAS.CODIGO_EMPRESA'] == plan['CD_EMPR']
        ],
        how='left'
    )
    
    table = final_df.select(
        'TABLE.*',
        'EMPRESAS.CODIGO_OPERADORA',
        'EMPRESAS.CODIGO_EMPRESA',
        'PLANOS.*'
    )
    
    return table

    
conn = SnowflakeConnector()
table = conn.get_table_from_snowflake('SINISTRO_HAPVIDA', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')
companies = conn.get_table_from_snowflake('EMPRESAS', 'AUXILIAR')
plans = conn.get_table_from_snowflake('PLANOS', 'AUXILIAR')
servers = conn.get_table_from_snowflake('PRESTADORES', 'AUXILIAR')
table = add_datetime_stamp(table,"DATE_TIME_SILVER_TO_GOLD")
table = generate_hospitalization_key(table)
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table)
table = generate_complex_treatment_key(table)
table = generate_emergency_care_key(table)
table = standardize_plans(table, companies, plans)
table = standardize_servers(table, servers)
conn.save_snowflake_table(table, 'HAPVIDA_CLASSIFICACAO', 'GOLD', 'Overwrite')
