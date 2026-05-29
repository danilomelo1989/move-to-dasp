# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union
import modules.gold.fastview.hospitalization_business_rules as hospitalization

from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat_ws, regexp_extract, lpad, regexp_replace, date_trunc, min, max, sum, datediff, floor
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def generate_hospitalization_key(claims):
    hospitalization_key = claims \
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

    claims = claims.join(
            hospitalization_key,
            'CODIGO_AUTORIZACAO',
            'LEFT',
        )

    return claims

conn = SnowflakeConnector()
claims = conn.get_table_from_snowflake('SINISTRO_ONE_HEALTH', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')
premium = conn.get_table_from_snowflake('PREMIO_ONE_HEALTH', 'DATABRICKS_SILVER')

claims = generate_hospitalization_key(claims)
claims = hospitalization.classify_events(claims, classifications)
claims = hospitalization.define_main_procedures(claims)
claims = hospitalization.define_entrance_type(claims, "CODIGO_CARTEIRINHA")
claims = claims.withColumn("CODIGO_GRUPO_ECONOMICO_OPERADORA", lit(""))
claims = hospitalization.generate_complex_treatment_key(claims, "CODIGO_CARTEIRINHA")
claims = hospitalization.generate_emergency_care_key(claims, "CODIGO_CARTEIRINHA")

CODIGO_OPERADORA = '62'
NOME_OPERADORA = 'ONE_HEALTH'

claims = fastview_union.define_carrier_code(claims, CODIGO_OPERADORA, NOME_OPERADORA)
claims = fastview_union.join_company_name(conn, claims)
claims = fastview_union.join_insurance_name(conn, claims)
claims = fastview_union.calculate_age(claims) 
claims = fastview_union.adapt_layout(claims, fastview_union.CLAIMS)
conn.save_snowflake_table(claims, 'SINISTRO_UNIFICADO', 'DEBUG', 'Overwrite')
