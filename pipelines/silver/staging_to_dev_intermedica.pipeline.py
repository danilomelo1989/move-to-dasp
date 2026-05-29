# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # INTERMEDICA

# COMMAND ----------

carrier = 'INTERMEDICA'
table_name = 'INTERMEDICA_CADASTRO_CADASTRO_DE_ASSOCIADOS'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'NOME_OPERADORA_ARQUIVO': {'type': 'string'},
    'CODIGO_CONTRATO': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'CODIGO_CPF': {'type': 'string'},
    'DESCRICAO_PARENTESCO': {'type': 'string'},
    'CODIGO_GENERO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
    'DATA_ADMISSAO': {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
    'DATA_INCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
    'CODIGO_ESTADO_CIVIL': {'type': 'string'},
    'DESCRICAO_CIDADE_BENEFICIARIO': {'type': 'string'},
    'UF_BENEFICIARIO': {'type': 'string'},
    'QUANTIDADE_BENEFICIARIO_ATIVO': {'type': 'integer'},
    'CODIGO_OPERADORA_ARQUIVO': {'type': 'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'DESCRICAO_CIDADE_EMPRESA': {'type': 'string'},
    'UF_EMPRESA': {'type': 'string'},
    'NOME_MAE': {'type': 'string'},
    'CODIGO_MATRICULA': {'type': 'string'},
    'BAIRRO_ENDERECO_BENEFICIARIO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'CODIGO_TIPO_ACOMODACAO_PLANO': {'type': 'string'},
    'CODIGO_CPF_TITULAR': {'type': 'string'},
    'CODIGO_CARTEIRINHA_TITULAR': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'CODIGO_FAMILIA': {'type': 'string'},
    'NOME_LOTACAO': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_CADASTRO_DE_ASSOCIADOS_INTERMEDICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'INTERMEDICA'
table_name = 'INTERMEDICA_CADASTRO_ESTADO_CIVIL'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'CADASTRO': {'type': 'string'},
    'CODIGO': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_ESTADO_CIVIL_INTERMEDICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'INTERMEDICA'
table_name = 'INTERMEDICA_CADASTRO_TIPO_DE_PARTICIPANTE'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'CODIGO': {'type': 'string'},
    'DESCRICAO': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_TIPO_DE_PARTICIPANTE_INTERMEDICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'INTERMEDICA'
table_name = 'INTERMEDICA_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'NOME_OPERADORA_ARQUIVO': {'type': 'string'},
    'CODIGO_CONTRATO': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
    'CODIGO_GENERO': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
    'DESCRICAO_TIPO_SINISTRO': {'type': 'string'},
    'VALOR_PAGO': {'type': 'money'},
    'DATA_PROCEDIMENTO': {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
    'DATA_REFERENCIA': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'CODIGO_PRESTADOR': {'type': 'string'},
    'NOME_PRESTADOR': {'type': 'string'},
    'DESCRICAO_ESPECIALIDADE': {'type': 'string'},
    'INDICA_PRESTADOR_PROPRIO': {'type': 'string'},
    'DESCRICAO_CIDADE_PRESTADOR_PROPRIO': {'type': 'string'},
    'DESCRICAO_UF_PRESTADOR_PROPRIO': {'type': 'string'},
    'CODIGO_PROCEDIMENTO': {'type': 'string'},
    'DESCRICAO_PROCEDIMENTO': {'type': 'string'},
    'CODIGO_CID': {'type': 'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type': 'integer'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'NUMERO_GUIA_CONTA': {'type': 'string'},
    'CODIGO_ESPECIALIDADE': {'type': 'string'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type': 'string'},
    'CODIGO_FAMILIA': {'type': 'string'},
    'CODIGO_CPF_TITULAR': {'type': 'string'},
    'CODIGO_TITULAR': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_INTERMEDICA', schema='DATABRICKS_SILVER', mode='Overwrite')
