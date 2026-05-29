# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
import modules.gold.fastview.fastview_union_rules as fastview_union 
from premium_union_rules import *
from pyspark.sql.functions import lit, col, expr, substring, length, when, to_date, concat_ws, regexp_extract, lpad, regexp_replace, month, add_months,trunc, sum ,floor

from pyspark.sql.dataframe import  *
#from pyspark.sql.column import *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def get_premium_sulamerica(conn):
    premium = conn.get_table_from_snowflake("PREMIO_SULAMERICA", 'DATABRICKS_SILVER')
    premium = premium.select(
        col("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        col("CODIGO_CONTRATO"),
        col("ANO_REFERENCIA"),
        col("MES_REFERENCIA"),
        to_date(concat_ws("-", "ANO_REFERENCIA", lpad(col("MES_REFERENCIA"), 2, "0"), lit("01")), "yyyy-MM-dd").alias("DATA_REFERENCIA"),
        lit("0").alias("CODIGO_PLANO"),
        when(col("CODIGO_LOCAL_EMPRESA_OPERADORA").substr(0, 1) == '0', col("CODIGO_LOCAL_EMPRESA_OPERADORA").substr(2, 5))
        .otherwise(col("CODIGO_LOCAL_EMPRESA_OPERADORA")).alias("CD_EMPR_GRPO"),
        lit(0).alias("CODIGO_TIPO_BENEFICIARIO"),
        lit(0).alias("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        lit("00").alias("CD_DIVISAO"),
        col("VALOR_PAGO"),
        lit(0).alias("VALOR_ACERTO"),
        lit(0).alias("VALOR_FRANQUIA"),
        lit(0).alias("VALOR_APORTE"),
        lit("0").alias("DESCRICAO_GENERO"),
        lit("0").alias("IDADE_BENEFICIARIO"),
        lit("0").alias("FAIXA_ETARIA"),
        lit("0").alias("DATA_INCLUSAO_PLANO"),
        lit("0").alias("DATA_EXCLUSAO_PLANO"),  
        lit("0").alias("PLANO_AON_2"), 
        # lit("0").alias("CODIGO_TIPO_BENEFICIARIO"), 
        lit("0").alias("CODIGO_OPERADORA"), 
        lit("0").alias("NOME_OPERADORA"),        
        col("ETL_SOURCE_FILE_NAME"),
        col("ETL_SOURCE_ZIP_NAME"),
        col("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        col("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER"),
        lit(current_timestamp()).alias("DATE_TIME_SILVER_TO_GOLD"),
        lit("CANCELADO").alias("STATUS"),          
        lit(0).alias("QUANTIDADE_BENEFICIARIO_ATIVO")
    )         
    return premium

def copay_rules(conn, premium):
    copay = conn.get_table_from_snowflake("PARTICIPACAO_SULAMERICA", 'DATABRICKS_SILVER')
    copay = copay.selectExpr(
        "SUBSTR(CODIGO_CONTRATO, 0, 5) as CODIGO_CONTRATO",
        "CODIGO_LOCAL_EMPRESA_OPERADORA",
        "VALOR_COPARTICIPACAO",
        "CAST(SUBSTR(ANO_MES_COPARTICIPACAO, 24, 2) as INTEGER) as MES_REFERENCIA",
        "CAST(SUBSTR(ANO_MES_COPARTICIPACAO, 27, 4) as INTEGER) as ANO_REFERENCIA"
    ).distinct()

    premium = premium.join(copay,
                                on=[
                                    "CODIGO_CONTRATO",
                                    "CODIGO_LOCAL_EMPRESA_OPERADORA",
                                    "ANO_REFERENCIA",
                                    "MES_REFERENCIA"
                                ], how="left") 

    return premium

def registration_to_premium_rules(registration):
    registration = registration.select(
        col("CODIGO_LOCAL_EMPRESA_OPERADORA"),
        col("CODIGO_CONTRATO"),
        lit(0).alias("ANO_REFERENCIA"),
        lit(0).alias("MES_REFERENCIA"),  
        trunc(add_months(to_date(col("DATA_REFERENCIA"),"yyyy-mm-dd") , -1), "month").alias("DATA_REFERENCIA"),
        col("CODIGO_PLANO"),
        lit(0).alias("CD_EMPR_GRPO"),
        col("DESCRICAO_ELEGIBILIDADE").alias("CODIGO_TIPO_BENEFICIARIO"),
        col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
        lit("00").alias("CD_DIVISAO"),
        lit(0).alias("VALOR_PAGO"),
        lit(0).alias("VALOR_ACERTO"),
        lit(0).alias("VALOR_FRANQUIA"),
        lit(0).alias("VALOR_APORTE"),
        col("DESCRICAO_GENERO"),
        col("DATA_NASCIMENTO"),
        floor(expr("datediff(DATE_TRUNC('MONTH', DATA_REFERENCIA), DATA_NASCIMENTO) / 365.25")).alias("IDADE_BENEFICIARIO"),
        lit(expr("CASE WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) IS NULL THEN 'N/C'            WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 18 THEN '00 a 18'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 23 then '19 a 23'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 28 then '24 a 28'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 33 then '29 a 33'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 38 then '34 a 38'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 43 then '39 a 43'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 48 then '44 a 48'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 53 then '49 a 53'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) <= 58 then '54 a 58'                         WHEN CAST(((DATE_TRUNC('MONTH', DATA_REFERENCIA)  - DATA_NASCIMENTO) / 365.25) as INTEGER) >= 59 then '59 ou +'                         ELSE 'N/C' END ")).alias("FAIXA_ETARIA"),
        to_date(col("DATA_INCLUSAO_PLANO"),"yyyy-mm-dd").alias("DATA_INCLUSAO_PLANO"),
        to_date(col("DATA_EXCLUSAO_PLANO"),"yyyy-mm-dd").alias("DATA_EXCLUSAO_PLANO"),
        when(to_date(col("DATA_INCLUSAO_PLANO"),"yyyy-mm-dd") >= trunc(add_months(to_date(col("DATA_REFERENCIA"),"yyyy-mm-dd") , -1), "month"), "CANCELADO")
        .when(trunc(add_months(to_date(col("DATA_REFERENCIA"),"yyyy-mm-dd") , -1), "month") < to_date(col("DATA_EXCLUSAO_PLANO"),"yyyy-mm-dd"), "ATIVO")
        .when(to_date(col("DATA_EXCLUSAO_PLANO"),"yyyy-mm-dd").isNull() , "ATIVO")
        .otherwise("CANCELADO").alias("STATUS"), 
        when(to_date(col("DATA_INCLUSAO_PLANO"),"yyyy-mm-dd") >= trunc(add_months(to_date(col("DATA_REFERENCIA"),"yyyy-mm-dd") , -1), "month"), 0)
        .when(trunc(add_months(to_date(col("DATA_REFERENCIA"),"yyyy-mm-dd") , -1), "month") < to_date(col("DATA_EXCLUSAO_PLANO"),"yyyy-mm-dd"), 1)
        .when(to_date(col("DATA_EXCLUSAO_PLANO"),"yyyy-mm-dd").isNull() , 1)
        .otherwise(0).alias("QUANTIDADE_BENEFICIARIO_ATIVO"), 
        col("PLANO_AON_2"),
        col("CODIGO_EMPRESA"),
        col("NOME_EMPRESA"),
        lit(0).alias("VALOR_COPARTICIPACAO"), 
        col("CODIGO_OPERADORA"),
        col("NOME_OPERADORA"),  
        lit(0).alias("ETL_SOURCE_FILE_NAME"),
        lit(0).alias("ETL_SOURCE_ZIP_NAME"),
        lit(0).alias("RUN_ID"),
        col("DATE_TIME_LOAD_STG"),
        lit(0).alias("SOURCE_FILE_NAME_LINE_NUMBER"),
        col("DATE_TIME_STG_TO_SILVER"),
        col("DATE_TIME_SILVER_TO_GOLD")
    )


    final = registration.groupBy(
                                registration.CODIGO_OPERADORA,
                                registration.CODIGO_CONTRATO,
                                registration.CODIGO_LOCAL_EMPRESA_OPERADORA, 
                                registration.ANO_REFERENCIA,
                                registration.MES_REFERENCIA,
                                registration.DATA_REFERENCIA,
                                registration.CODIGO_PLANO,
                                registration.CD_EMPR_GRPO,
                                registration.CODIGO_TIPO_BENEFICIARIO,
                                registration.CODIGO_GRUPO_ECONOMICO_OPERADORA,
                                registration.CD_DIVISAO,
                                registration.VALOR_PAGO,
                                registration.VALOR_ACERTO,
                                registration.VALOR_FRANQUIA,
                                registration.VALOR_APORTE,
                                registration.DESCRICAO_GENERO,
                                registration.IDADE_BENEFICIARIO,
                                registration.FAIXA_ETARIA,
                                registration.DATA_INCLUSAO_PLANO,
                                registration.DATA_EXCLUSAO_PLANO,
                                registration.PLANO_AON_2,
                                registration.CODIGO_EMPRESA,
                                registration.NOME_EMPRESA,
                                # registration.CODIGO_TIPO_BENEFICIARIO,
                                registration.NOME_OPERADORA,
                                registration.ETL_SOURCE_FILE_NAME,
                                registration.ETL_SOURCE_ZIP_NAME,
                                registration.RUN_ID,
                                registration.DATE_TIME_LOAD_STG,
                                registration.SOURCE_FILE_NAME_LINE_NUMBER,
                                registration.DATE_TIME_STG_TO_SILVER,
                                registration.DATE_TIME_SILVER_TO_GOLD,
                                registration.STATUS
                                ).agg(sum(registration.QUANTIDADE_BENEFICIARIO_ATIVO).alias("QUANTIDADE_BENEFICIARIO_ATIVO"))
    return registration

CODIGO_OPERADORA = '9'
NOME_OPERADORA = 'SULAMERICA'
conn = SnowflakeConnector()

premium = get_premium_sulamerica(conn)
premium = copay_rules(conn, premium)
premium = fastview_union.define_carrier_code(premium, CODIGO_OPERADORA, NOME_OPERADORA)
premium = fastview_union.join_company_name(conn, premium)

registration = conn.get_table_from_snowflake("CADASTRO_SULAMERICA", 'DATABRICKS_SILVER')
registration = fastview_union.define_carrier_code(registration, CODIGO_OPERADORA, NOME_OPERADORA)
registration = fastview_union.join_company_name(conn, registration)
registration = fastview_union.join_insurance_name(conn, registration)
registration = registration_to_premium_rules(registration)

columns = premium.columns
registration = registration.select(columns)
premium = registration.union(premium)

table = fastview_union.adapt_layout(premium, fastview_union.PREMIUM)
conn.save_snowflake_table(table, 'PREMIO_UNIFICADO', 'DEBUG', 'Overwrite')
