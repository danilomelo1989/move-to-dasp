# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union
import modules.gold.fastview.hospitalization_business_rules as hospitalization
from modules.gold.fastview.hospitalization_business_rules import *


from pyspark.sql.functions import row_number, min, max, mean, datediff, col, lag, when, sum, array_contains, coalesce, lit, first, last, concat
from pyspark.sql.window import Window

def generate_hospitalization_key(table):

    window_spec = Window.partitionBy("CODIGO_BENEFICIARIO").orderBy("DATA_PROCEDIMENTO")
    window_spec2 = Window.partitionBy("CODIGO_BENEFICIARIO").orderBy("DATA_PROCEDIMENTO").rowsBetween(Window.unboundedPreceding, 0)

    hospitalization_key_A = table \
        .filter('INDICA_INTERNACAO = "S"') \
        .orderBy("CODIGO_BENEFICIARIO", "DATA_PROCEDIMENTO") \
        .withColumn("lagged_date_diff", datediff(col("DATA_PROCEDIMENTO"), lag("DATA_PROCEDIMENTO", 1).over(window_spec))) \
        .withColumn("start_marker", when(col("lagged_date_diff").isNull() | (col("lagged_date_diff") > 1), 1).otherwise(0)) \
        .withColumn("CHAVE_INTERNACAO_AON", sum("start_marker").over(window_spec2)) \
        .groupBy("CODIGO_BENEFICIARIO", "CHAVE_INTERNACAO_AON") \
        .agg(
            sum("VALOR_PAGO").alias("CUSTO_TOTAL_INTERNACAO"),
            min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            max("DATA_PROCEDIMENTO").alias("FINAL_INTERNACAO")
        ) 

    hospitalization_key_B = table \
        .filter('INDICA_INTERNACAO = "S"') \
        .orderBy("CODIGO_BENEFICIARIO", "DATA_PROCEDIMENTO") \
        .withColumn("lagged_date_diff", datediff(col("DATA_PROCEDIMENTO"), lag("DATA_PROCEDIMENTO", 1).over(window_spec))) \
        .withColumn("start_marker", when(col("lagged_date_diff").isNull() | (col("lagged_date_diff") > 1), 1).otherwise(0)) \
        .withColumn("CHAVE_INTERNACAO_AON", sum("start_marker").over(window_spec2)) \

    hospitalization_key = hospitalization_key_B.join(
            hospitalization_key_A,
            (hospitalization_key_B["CHAVE_INTERNACAO_AON"] == hospitalization_key_A["CHAVE_INTERNACAO_AON"]) & (hospitalization_key_B["CODIGO_BENEFICIARIO"] == hospitalization_key_A["CODIGO_BENEFICIARIO"]),
            'LEFT'
        ) \
        .select(
            hospitalization_key_B["CHAVE_INTERNACAO_AON"],
            hospitalization_key_B["DATA_PROCEDIMENTO"],
            hospitalization_key_B["CODIGO_BENEFICIARIO"],
            hospitalization_key_B["INDICA_INTERNACAO"],
            hospitalization_key_A["CUSTO_TOTAL_INTERNACAO"],
            hospitalization_key_A["INICIO_INTERNACAO"],
            hospitalization_key_A["FINAL_INTERNACAO"],
            lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    hospitalization_key = hospitalization_key \
        .groupBy("CODIGO_BENEFICIARIO", "DATA_PROCEDIMENTO") \
        .agg(
            mean("CHAVE_INTERNACAO_AON").alias("CHAVE_INTERNACAO_AON"),
            mean("CUSTO_TOTAL_INTERNACAO").alias("CUSTO_TOTAL_INTERNACAO"),
            max("INICIO_INTERNACAO").alias("INICIO_INTERNACAO"),
            max("FINAL_INTERNACAO").alias("FINAL_INTERNACAO"),
            max("NOME_EVENTO").alias("NOME_EVENTO"),
            max("INDICA_INTERNACAO").alias("INDICA_INTERNACAO")
        ) \
        .withColumn("CHAVE_INTERNACAO_AON", concat("CODIGO_BENEFICIARIO", "CHAVE_INTERNACAO_AON")) \
        .withColumn("PERIODO_INTERNACAO", datediff(col("FINAL_INTERNACAO"), col("INICIO_INTERNACAO"))+1) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500")  

    table = table.join(
            hospitalization_key,
            (table["CODIGO_BENEFICIARIO"] == hospitalization_key["CODIGO_BENEFICIARIO"]) & (table["DATA_PROCEDIMENTO"] == hospitalization_key["DATA_PROCEDIMENTO"]) &  (table["INDICA_INTERNACAO"] == hospitalization_key["INDICA_INTERNACAO"]) ,
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
        ) \
        .withColumn("CODIGO_AUTORIZACAO", col("CHAVE_INTERNACAO_AON"))
        

    return table

def removing_leading_zeros(table):
    table = table.withColumn('CODIGO_PROCEDIMENTO', regexp_replace('CODIGO_PROCEDIMENTO', r'^[0]*', ''))
    return table


conn = SnowflakeConnector()
claims = conn.get_table_from_snowflake('SINISTRO_SEGUROS_UNIMED', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')

claims = generate_hospitalization_key(claims)
claims = hospitalization.classify_events(claims, classifications)
claims = hospitalization.define_main_procedures(claims)
claims = hospitalization.define_entrance_type(claims)
claims = hospitalization.generate_complex_treatment_key(claims)
claims = hospitalization.generate_emergency_care_key(claims)

CODIGO_OPERADORA = '14'
NOME_OPERADORA = 'SEGUROS UNIMED'

claims = fastview_union.define_carrier_code(claims )
claims = fastview_union.join_company_name(conn, claims)
claims = fastview_union.join_insurance_name(conn, claims)
table = fastview_union.adapt_layout(claims, fastview_union.CLAIMS)
conn.save_snowflake_table(table, 'SINISTRO_UNIFICADO_UNIMED', 'DEBUG', 'append')


