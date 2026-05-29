# Databricks notebook source
# MAGIC %pip install tabula-py
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *
from pyspark.sql.functions import row_number, min, max, mean, datediff, col, lag, when, sum, array_contains, coalesce, lit, first, last, concat
from pyspark.sql.window import Window

def generate_hospitalization_key(table):

    window_spec = Window.partitionBy("CODIGO_CARTEIRINHA").orderBy("DATA_PROCEDIMENTO")
    window_spec2 = Window.partitionBy("CODIGO_CARTEIRINHA").orderBy("DATA_PROCEDIMENTO").rowsBetween(Window.unboundedPreceding, 0)

    hospitalization_key_A = table \
        .filter('INDICA_INTERNACAO = "S"') \
        .orderBy("CODIGO_CARTEIRINHA", "DATA_PROCEDIMENTO") \
        .withColumn("lagged_date_diff", datediff(col("DATA_PROCEDIMENTO"), lag("DATA_PROCEDIMENTO", 1).over(window_spec))) \
        .withColumn("start_marker", when(col("lagged_date_diff").isNull() | (col("lagged_date_diff") > 1), 1).otherwise(0)) \
        .withColumn("CHAVE_INTERNACAO_AON", F.sum("start_marker").over(window_spec2)) \
        .groupBy("CODIGO_CARTEIRINHA", "CHAVE_INTERNACAO_AON") \
        .agg(
            F.sum("VALOR_APRESENTADO").alias("CUSTO_TOTAL_INTERNACAO"),
            F.min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            F.max("DATA_PROCEDIMENTO").alias("FINAL_INTERNACAO")
        ) 

    hospitalization_key_B = table \
        .filter('INDICA_INTERNACAO = "S"') \
        .orderBy("CODIGO_CARTEIRINHA", "DATA_PROCEDIMENTO") \
        .withColumn("lagged_date_diff", datediff(col("DATA_PROCEDIMENTO"), lag("DATA_PROCEDIMENTO", 1).over(window_spec))) \
        .withColumn("start_marker", when(col("lagged_date_diff").isNull() | (col("lagged_date_diff") > 1), 1).otherwise(0)) \
        .withColumn("CHAVE_INTERNACAO_AON", F.sum("start_marker").over(window_spec2)) \

    hospitalization_key = hospitalization_key_B.join(
            hospitalization_key_A,
            (hospitalization_key_B["CHAVE_INTERNACAO_AON"] == hospitalization_key_A["CHAVE_INTERNACAO_AON"]) & (hospitalization_key_B["CODIGO_CARTEIRINHA"] == hospitalization_key_A["CODIGO_CARTEIRINHA"]),
            'LEFT'
        ) \
        .select(
            hospitalization_key_B["CHAVE_INTERNACAO_AON"],
            hospitalization_key_B["DATA_PROCEDIMENTO"],
            hospitalization_key_B["CODIGO_CARTEIRINHA"],
            hospitalization_key_B["INDICA_INTERNACAO"],
            hospitalization_key_A["CUSTO_TOTAL_INTERNACAO"],
            hospitalization_key_A["INICIO_INTERNACAO"],
            hospitalization_key_A["FINAL_INTERNACAO"],
            F.lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    hospitalization_key = hospitalization_key \
        .groupBy("CODIGO_CARTEIRINHA", "DATA_PROCEDIMENTO") \
        .agg(
            F.mean("CHAVE_INTERNACAO_AON").alias("CHAVE_INTERNACAO_AON"),
            F.mean("CUSTO_TOTAL_INTERNACAO").alias("CUSTO_TOTAL_INTERNACAO"),
            F.max("INICIO_INTERNACAO").alias("INICIO_INTERNACAO"),
            F.max("FINAL_INTERNACAO").alias("FINAL_INTERNACAO"),
            F.max("NOME_EVENTO").alias("NOME_EVENTO"),
            F.max("INDICA_INTERNACAO").alias("INDICA_INTERNACAO")
        ) \
        .withColumn("CHAVE_INTERNACAO_AON", concat("CODIGO_CARTEIRINHA", "CHAVE_INTERNACAO_AON")) \
        .withColumn("PERIODO_INTERNACAO", datediff(col("FINAL_INTERNACAO"), col("INICIO_INTERNACAO"))+1) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500")  

    table = table.join(
            hospitalization_key,
            (table["CODIGO_CARTEIRINHA"] == hospitalization_key["CODIGO_CARTEIRINHA"]) & (table["DATA_PROCEDIMENTO"] == hospitalization_key["DATA_PROCEDIMENTO"]) &  (table["INDICA_INTERNACAO"] == hospitalization_key["INDICA_INTERNACAO"]) ,
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
        .withColumn("CODIGO_AUTORIZACAO", 
                   when(col("CHAVE_INTERNACAO_AON").isNotNull(), col("CHAVE_INTERNACAO_AON"))
                   .otherwise(col("CODIGO_AUTORIZACAO"))) \
        .withColumn("CODIGO_BENEFICIARIO", col("CODIGO_CARTEIRINHA"))
        

    return table

def define_main_procedures(table):
    #TODO Pode acontecer de a internacao n ter nenhum proc_princ, tratar esses casos 
    main_procedure = table \
        .filter('PROC_PRINC = 1.0 AND NOME_EVENTO = "INTERNAÇÃO"') \
        .withColumn(
            "rank", F.row_number().over(Window.partitionBy("CODIGO_AUTORIZACAO").orderBy(F.desc("VALOR_APRESENTADO")))
        ).filter("rank = 1") \
        .select(
            F.col("CODIGO_AUTORIZACAO"), 
            F.col("COD_TUSS").alias('PROCEDIMENTO_PRINCIPAL'), 
            F.when(
                F.col("TIPO_INTERNACAO").isNotNull(),
                F.col("TIPO_INTERNACAO")
            ).otherwise("Clínica") \
            .alias('TIPO_INTERNACAO') 
        )

    #TODO Checar se codigo_autorizacao e chave_internacao_aon dao diferentes resultados
    columns = list(set(table.columns) - {'TIPO_INTERNACAO'})
    table = table.select(columns).join(
        main_procedure,
        'CODIGO_AUTORIZACAO',
        'LEFT'
    )    

    return table
    
conn = SnowflakeConnector()
table = conn.get_table_from_snowflake('SINISTRO_CENTRAL_NACIONAL_UNIMED', 'DATABRICKS_SILVER')
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
conn.save_snowflake_table(table, 'CENTRAL_NACIONAL_UNIMED_CLASSIFICACAO', 'GOLD', 'Overwrite')
