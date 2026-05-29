from modules.constants import DEBUG
from modules.utils.helpers import execute_itermediate_steps
from datetime import datetime
from modules.utils.connectors import SnowflakeConnector
from pyspark.sql.functions import lit, col, current_timestamp, date_format
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType


unified_schema = StructType([
    StructField("CODIGO_OPERADORA", StringType(), True),
    StructField("NOME_OPERADORA", StringType(), True),
    StructField("CODIGO_CONTRATO", StringType(), True),
    StructField("CODIGO_GRUPO_ECONOMICO_OPERADORA", StringType(), True),
    StructField("CODIGO_LOCAL_EMPRESA_OPERADORA", StringType(), True),
    StructField("NOME_LOCAL_EMPRESA_OPERADORA", StringType(), True),
    StructField("CODIGO_BENEFICIARIO", StringType(), True),
    StructField("CODIGO_FAMILIA", StringType(), True),
    StructField("NOME_BENEFICIARIO", StringType(), True),
    StructField("CODIGO_CPF_TITULAR", StringType(), True),
    StructField("CODIGO_CPF_DEPENDENTE", StringType(), True),
    StructField("DATA_NASCIMENTO", DateType(), True),
    StructField("DESCRICAO_GENERO", StringType(), True),
    StructField("CODIGO_PARENTESCO", StringType(), True),
    StructField("NOME_MAE", StringType(), True),
    StructField("CODIGO_PLANO", StringType(), True),
    StructField("DATA_INCLUSAO_PLANO", DateType(), True),
    StructField("DATA_EXCLUSAO_PLANO", DateType(), True),
    StructField("DESCRICAO_ELEGIBILIDADE", StringType(), True),
    StructField("DATA_REFERENCIA", DateType(), True),
    StructField("ETL_SOURCE_FILE_NAME", StringType(), True),
    StructField("ETL_SOURCE_ZIP_NAME", StringType(), True),
    StructField("RUN_ID", StringType(), True),
    StructField("DATE_TIME_LOAD_STG", TimestampType(), True),
    StructField("SOURCE_FILE_NAME_LINE_NUMBER", StringType(), True),
    StructField("DATE_TIME_STG_TO_SILVER", TimestampType(), True),
    StructField("DATE_TIME_SILVER_TO_GOLD", TimestampType(), True)
])

def adapt_layout(df, schema):
    select_columns = []
    for field in schema.fields:
        if field.name in df.columns:
            select_columns.append(col(field.name))
        else:
            select_columns.append(lit(None).cast(StringType()).alias(field.name))
    return df.select(*select_columns)

def append_to_snowflake_table(conn, df, table_name, schema_name):
    adapted_df = adapt_layout(df, unified_schema)    
    conn.save_snowflake_table(adapted_df, table_name, schema_name, 'Append')

def process_and_append_tables(conn, table_names, schema, codigo_operadora = '', nome_operadora = ''):
    for table_name in table_names:
        df = conn.get_table_from_snowflake(table_name, 'DATABRICKS_SILVER')
        df = df.withColumn("DATE_TIME_SILVER_TO_GOLD", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
        df = df.withColumn("CODIGO_OPERADORA", lit(codigo_operadora))
        df = df.withColumn("NOME_OPERADORA", lit(nome_operadora))
        append_to_snowflake_table(conn, df, 'TESTE_CADASTRO_UNIFICADO', 'GOLD')

