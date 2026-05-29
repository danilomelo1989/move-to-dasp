from modules.utils.helpers import execute_itermediate_steps
from modules.utils.connectors import SnowflakeConnector
from modules.gold.fastview.hospitalization_business_rules import *

from pyspark.sql import functions


def set_claims_table():
    conn = SnowflakeConnector()
    header = conn.get_table_from_snowflake('sinistro_h_bradesco', 'databricks_silver')
    middle = conn.get_table_from_snowflake('sinistro_m_bradesco', 'databricks_silver')
    service_description = conn.get_table_from_snowflake('BRADESCO_DESCRICAO_SERVICO', 'auxiliar')
    
    header = header \
        .select(
            "CODIGO_CONTRATO",
            "DATA_REFERENCIA",
            "ETL_SOURCE_FILE_NAME"
        )
    
    table = middle \
        .join(
            header,
            'ETL_SOURCE_FILE_NAME',
            'LEFT'
        )
    
    table = table \
        .join(
            service_description,
            'CODIGO_PROCEDIMENTO',
            'LEFT'
        )
    
    return table

def get_necessary_columns(table):
    table = table \
        .select(
            "CODIGO_CONTRATO",
            functions.col("codigo_empresa_operadora") \
                .alias("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
            "DATA_PROCEDIMENTO",
            functions.concat("CODIGO_FAMILIA", "CODIGO_DEPENDENCIA") \
                .alias("CODIGO_BENEFICIARIO"),
            "DESCRICAO_GENERO",
            "IDADE_BENEFICIARIO",
            functions.when(
                    functions.col("CODIGO_DEPENDENCIA") == "00", "TITULAR"
                ) \
                .otherwise(functions.lit("DEPENDENTE"))
            
            "CODIGO_PROCEDIMENTO",
            "DESCRICAO_PROCEDIMENTO",
            "VALOR_PAGO",
            "QUANTIDADE_PROCEDIMENTO",
            "CODIGO_TIPO_SINISTRO",
            "CODIGO_SINISTRO",
            functions.col("CODIGO_GRUPO_ECONOMICO_OPERADORA") \
                .alias("CODIGO_EMPRESA_OPERADORA"),
            "DATA_REFERENCIA",
            "CODIGO_PRESTADOR",
            "NOME_PRESTADOR"
        )
        
    return table

@execute_itermediate_steps(False)
def generate_hospitalization_key(table):
    hospitalization_key = table.filter('''
            CODIGO_TIPO_SINISTRO = 3 OR CODIGO_TIPO_SINISTRO = 4    
        ''') \
        .groupBy("CODIGO_SINISTRO") \
        .agg(
            functions.min("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),
            functions.sum("VALOR_PAGO").alias("CUSTO_TOTAL_INTERNACAO")
        ) \
        .filter("CUSTO_TOTAL_INTERNACAO >= 500") \
        .select(
            "CODIGO_SINISTRO",
            functions.col("CODIGO_SINISTRO").alias('CHAVE_INTERNACAO_AON'),
            "INICIO_INTERNACAO", 
            "CUSTO_TOTAL_INTERNACAO",
            functions.lit("INTERNAÇÃO").alias("NOME_EVENTO")
        )

    table = table.join(
            hospitalization_key,
            'CODIGO_SINISTRO',
            'LEFT',
        )

    return table

table = set_claims_table()
table = get_necessary_columns(table)
table = generate_hospitalization_key(table)

conn = SnowflakeConnector()
classifications = conn.get_table_from_snowflake('TB_AUX_PROCEDIMENTOS', 'DATABRICKS_SILVER')
table = classify_events(table, classifications)
table = define_main_procedures(table)
table = define_entrance_type(table)
table = generate_complex_treatment_key(table)
table = generate_emergency_care_key(table)

conn.save_snowflake_table(table, 'BRADESCO_CLASSIFICACAO', 'GOLD', 'Overwrite')
