from modules.utils.helpers import execute_itermediate_steps
from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *

@execute_itermediate_steps(False)
def generate_hospitalization_key(table):
    hospitalization_key = table \
        .filter('CODIGO_AUTORIZACAO != "nan"') \
        .filter('''
            DESCRICAO_TIPO_CATEGORIA_SINISTRO = "INTERNADO-APTO"
            OR DESCRICAO_TIPO_CATEGORIA_SINISTRO = "INTERNADO-ENFERMARIA"
            OR DESCRICAO_TIPO_CATEGORIA_SINISTRO = "HOSPITAL DIA"
        ''') \
        .groupBy("CODIGO_BENEFICIARIO", "CODIGO_AUTORIZACAO") \
        .agg(   #ADICIONAR CASE WHEN PARA CASOS QUE DATA_PROCEDIMENTO ESTA VAZIO
            functions.min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            functions.sum("VALOR_PAGO").alias("CUSTO_TOTAL_INTERNACAO")
        ) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500") \
        .select(
            "CODIGO_AUTORIZACAO",
            functions.col("CODIGO_AUTORIZACAO").alias('CHAVE_INTERNACAO_AON'), 
            "INICIO_INTERNACAO", 
            "CUSTO_TOTAL_INTERNACAO",
            functions.lit("INTERNACAO").alias("NOME_EVENTO")
        )

    table = table.join(
            hospitalization_key,
            'CODIGO_AUTORIZACAO',
            'LEFT'
        )

    return table

conn = SnowflakeConnector()
table_p = conn.get_table_from_snowflake('SINISTRO_SULAMERICA', 'DATABRICKS_SILVER')
classifications = conn.get_table_from_snowflake('TB_AUX_PROCEDIMENTOS', 'DATABRICKS_SILVER')

# table = generate_generic_hospitalization_key(table_p)

table = generate_hospitalization_key(table_p)
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table)
table = generate_complex_treatment_key(table)
table = generate_emergency_care_key(table)

# conn.save_snowflake_table(table, 'SULAMERICA_CLASSIFICACAO', 'GOLD', 'Overwrite')
