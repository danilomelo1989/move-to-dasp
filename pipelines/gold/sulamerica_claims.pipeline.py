# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union
import modules.gold.fastview.hospitalization_business_rules as hospitalization

from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat_ws, regexp_extract, lpad, regexp_replace, date_trunc, min, sum
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def generate_hospitalization_key(table):
    hospitalization_key = table \
        .filter('CODIGO_AUTORIZACAO != "nan"') \
        .filter('''
            DESCRICAO_TIPO_CATEGORIA_SINISTRO = "INTERNADO-APTO"
            OR DESCRICAO_TIPO_CATEGORIA_SINISTRO = "INTERNADO-ENFERMARIA"
            OR DESCRICAO_TIPO_CATEGORIA_SINISTRO = "HOSPITAL DIA"
        ''') \
        .groupBy("CODIGO_AUTORIZACAO") \
        .agg(
            min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            sum("VALOR_PAGO").alias("CUSTO_TOTAL_INTERNACAO")
        ) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500") \
        .select(
            "CODIGO_AUTORIZACAO",
            col("CODIGO_AUTORIZACAO").alias('CHAVE_INTERNACAO_AON'), 
            "INICIO_INTERNACAO", 
            "CUSTO_TOTAL_INTERNACAO",
            lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    table = table.join(
            hospitalization_key,
            'CODIGO_AUTORIZACAO',
            'LEFT'
        )

    return table


def select_fields_claims(df):
    # Perform the joins and transformations
    df = df.select(
        col("DATA_REFERENCIA"),
        col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        col("CODIGO_PRESTADOR"),
        col("NOME_PRESTADOR"),        
        col("NOME_EVENTO"),
        col("DESCRICAO_TIPO_CATEGORIA_SINISTRO"),
        col("DESCRICAO_POSICAO_PRESTADOR"),
        col("CODIGO_CNPJ_PRESTADOR"),
        col("CODIGO_PLANO"),        
        col("CODIGO_PROCEDIMENTO"),
        col("DESCRICAO_PROCEDIMENTO"),
        col("TIPO_ATENDIMENTO"),
        col("INICIO_INTERNACAO"),
        col("CHAVE_INTERNACAO_AON"),
        col("DATA_PROCEDIMENTO"),
        col("VALOR_PAGO"),
        col("VALOR_COPARTICIPACAO"),
        col("QUANTIDADE_PROCEDIMENTO"),
        col("CHAVE_TERAPIA_COMPLEXA_AON"),
        col("CHAVE_PRONTO_SOCORRO_AON"),
        col("CODIGO_BENEFICIARIO"),
        col("NM_EVNT2"),
        col("DS_SRVC_2"),
        col("DS_SRVC_ORIGINAL"),
        col("COD_TUSS"),
        col("COD_CBHPM"),
        col("PROC_PRINC"),
        col("CATEGORIA"),
        col("TIPO_INTERNACAO"),
        col("TIPO_ENTRADA_INTERNACAO"),
        col("REGIME_DIARIA"),
        col("CRONICO"),
        col("GRUPO"),
        col("CODIGO_CONTRATO"),
        col("DS_SRVC_ABREVIADA"),
        col("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        col("DESCRICAO_GENERO"),
        col("IDADE_BENEFICIARIO"),
        col("DESCRICAO_ELEGIBILIDADE"),
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER")
    )

    return df

conn = SnowflakeConnector()
claims = conn.get_table_from_snowflake('SINISTRO_SULAMERICA', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')

claims = generate_hospitalization_key(claims)
claims = hospitalization.classify_events(claims, classifications)
claims = hospitalization.define_main_procedures(claims)
claims = hospitalization.define_entrance_type(claims)
claims = hospitalization.generate_complex_treatment_key(claims)
claims = hospitalization.generate_emergency_care_key(claims)

CODIGO_OPERADORA = '9'
NOME_OPERADORA = 'SULAMERICA'

# claims = select_fields_claims(claims)
claims = fastview_union.define_carrier_code(claims,CODIGO_OPERADORA ,NOME_OPERADORA )
claims = fastview_union.join_company_name(conn, claims)
claims = fastview_union.join_insurance_name(conn, claims)
claims = fastview_union.adapt_layout(claims, fastview_union.CLAIMS)
conn.save_snowflake_table(claims, 'SINISTRO_UNIFICADO_DANILO2', 'DEBUG', 'append')
