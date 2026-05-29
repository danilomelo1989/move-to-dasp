# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *
from pyspark.sql.functions import lit, col, expr, when, floor
import pyspark.sql.functions as functions

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def generate_hospitalization_key(
    table,
):
    columns = table.columns
    hospitalization_key = table \
        .select(
            "CODIGO_CONTRATO",
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "CODIGO_BENEFICIARIO",
            "DATA_PROCEDIMENTO",
            functions \
                .when(
                    functions.col('DESCRICAO_TIPO_CATEGORIA_SINISTRO') == 'S',
                    functions.concat(
                        functions.lit('INT'),
                        functions.col("CODIGO_CONTRATO"),
                        functions.col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
                        functions.col("CODIGO_BENEFICIARIO"),
                        functions.col("DATA_PROCEDIMENTO")
                    )
                ) \
                .otherwise(functions.lit(None)) \
                .alias("CHAVE_INTERNACAO_AON")
        ) \
        .distinct()

    columns = hospitalization_key.columns
    window_spec = Window \
        .partitionBy(
            "CODIGO_CONTRATO", 
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "CODIGO_BENEFICIARIO"
        ) \
        .orderBy(
            "DATA_PROCEDIMENTO"
        )
    lead_date = hospitalization_key \
        .select(
            *columns,
            functions.lead("DATA_PROCEDIMENTO").over(window_spec).alias("NEXT_DAY")
        )

    hospitalization_keys = lead_date \
        .repartition("CODIGO_CONTRATO", "CODIGO_GRUPO_ECONOMICO_OPERADORA") \
        .orderBy(
            "CODIGO_CONTRATO", 
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "CODIGO_BENEFICIARIO",
            "DATA_PROCEDIMENTO"
        ) \
        .rdd \
        .mapPartitions(group_by_days) \
        .toDF([
            "UDF_CODIGO_CONTRATO", 
            "UDF_CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "UDF_CODIGO_BENEFICIARIO", 
            "UDF_INTERNACAO_AON",
            "INICIO_INTERNACAO",
            "FIM_INTERNACAO"
        ])

    window_total_hospitalization  = Window.partitionBy("UDF_INTERNACAO_AON")
    columns = table.columns
    table = table \
        .join(
            hospitalization_keys,
            (table['CODIGO_CONTRATO'] == hospitalization_keys['UDF_CODIGO_CONTRATO'])
            & (table['CODIGO_GRUPO_ECONOMICO_OPERADORA'] == hospitalization_keys['UDF_CODIGO_GRUPO_ECONOMICO_OPERADORA'])
            & (table['CODIGO_BENEFICIARIO'] == hospitalization_keys['UDF_CODIGO_BENEFICIARIO'])
            & (
                table['DATA_PROCEDIMENTO'].between(
                    hospitalization_keys['INICIO_INTERNACAO'], 
                    hospitalization_keys['FIM_INTERNACAO']
                )
            ),
            'LEFT'
        ) \
        .select(
            *columns,
            functions.sum("VALOR_PAGO").over(window_total_hospitalization).alias("CUSTO_TOTAL_INTERNACAO"),
            functions \
                .when(functions.col("CUSTO_TOTAL_INTERNACAO") > "500", functions.col("UDF_INTERNACAO_AON")) \
                .otherwise(functions.lit(None)) \
                .alias('CHAVE_INTERNACAO_AON'),
            functions \
                .when(functions.col("CHAVE_INTERNACAO_AON").isNotNull(), functions.lit("INTERNACAO")) \
                .otherwise(functions.lit(None)) \
                .alias("NOME_EVENTO"),
            "INICIO_INTERNACAO"
        )

    return table


def get_sas_claims(carrier):
    table = conn.get_table_from_snowflake(f'{carrier}_TB_UTLZ', 'SAS_SILVER')
    provider = conn.get_table_from_snowflake(f'{carrier}_TB_PRESTADOR_HASH', 'SAS_SILVER')
    users = conn.get_table_from_snowflake(f'{carrier}_TB_USROU_HASH', 'SAS_SILVER')
    company = conn.get_table_from_snowflake(f'{carrier}_TB_OP_EMP', 'SAS_SILVER')
    claims = conn.get_table_from_snowflake(f'{carrier}_TB_SRVC_HASH', 'SAS_SILVER')
    
    table = table.join(
        provider,
        ['PREST_HASH'],
        'LEFT'
    ) \
    .join(
        users,
        ['USRO_HASH'],
        'LEFT'
    ) \
    .join(
        company,
        ['DT_REFR', 'CD_OPRD', 'CD_EMPR'],
        'LEFT'
    ) \
    .join(
        claims,
        ['SRVC_HASH'],
        'LEFT'
    ) \
    .select(
        col("CD_OPRD").alias("CODIGO_OPERADORA"),
        col("NM_OPRD").alias("NOME_OPERADORA"),
        col("CD_EMPR").alias("CODIGO_EMPRESA"),
        col("NM_EMPR").alias("NOME_EMPRESA"),
        col("DT_REFR").alias("DATA_REFERENCIA"),
        col("CD_APLC").alias("CODIGO_CONTRATO"),
        col("CD_EMPR_GRPO").alias("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        col("CD_LCAL_UTLZ").alias("CODIGO_PRESTADOR"),
        col("NM_LCAL_UTLZ").alias("NOME_PRESTADOR"),
        col("CD_PLNO").alias("CODIGO_PLANO"),
        col("FL_INTD").alias("DESCRICAO_TIPO_CATEGORIA_SINISTRO"),
        col("CD_SRVC").alias("CODIGO_PROCEDIMENTO"),
        col("DS_SRVC").alias("DESCRICAO_PROCEDIMENTO"),
        col("NM_REDE_RMBL").alias("TIPO_ATENDIMENTO"),
        col("DT_ATND").alias("DATA_PROCEDIMENTO"),
        col("VL_PAGO_EVNT").alias("VALOR_PAGO"),
        col("VL_CO_PART").alias("VALOR_COPARTICIPACAO"),
        col("QT_SRVC").alias("QUANTIDADE_PROCEDIMENTO"),
        col("CD_DPND").alias("CODIGO_BENEFICIARIO"),
        col("FL_SEXO").alias("DESCRICAO_GENERO"),
        floor(expr("datediff(DT_REFR, DT_NSCM) / 365.25")).alias("IDADE_BENEFICIARIO"),
        when(
            col("CD_TTLR") == col("CD_DPND"), lit('T')
        ) \
        .otherwise(lit('D')) \
        .alias("DESCRICAO_ELEGIBILIDADE")
    ) \
    .filter(col("DATA_REFERENCIA").between('2022-01-01', '2022-12-31'))
    
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
    ) \
    
    df = df.withColumn("CODIGO_OPERADORA", lit(codigo_operadora)) \
            .withColumn("NOME_OPERADORA", lit(nome_operadora)) \
            .withColumn("DATE_TIME_SILVER_TO_GOLD", current_timestamp())
                                                                                                 
                    
    append_to_snowflake_table(conn, df, 'TESTE_SINISTRO_UNIFICADO', 'GOLD')


conn = SnowflakeConnector()
# table = conn.get_table_from_snowflake('SINISTRO_AMIL', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('PROCEDIMENTOS', 'AUXILIAR')

table = get_sas_claims('SULAMERICA')
table = generate_hospitalization_key(table)
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table)
table = generate_complex_treatment_key(table)
table = generate_emergency_care_key(table)

append_to_snowflake_table(conn, table, 'CADASTRO_UNIFICADO', 'GOLD')
