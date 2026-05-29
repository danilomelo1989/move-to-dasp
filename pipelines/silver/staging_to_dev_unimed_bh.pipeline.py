# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # UNIMED BH

# COMMAND ----------

carrier = 'UNIMED_BH'
table_name = 'UNIMED_BH_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'CODIGO_CONTRATO': {'type': 'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
    'CODIGO_MATRICULA': {'type': 'string'},
    'CODIGO_MATRICULA_ESPECIAL': {'type': 'string'},
    'CODIGO_TITULAR': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'DESCRICAO_CARGO': {'type': 'string'},
    'INDICA_EXAME_ADMISSIONAL': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'CODIGO_CPF': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/mm/yyyy"},
    'IDADE_BENEFICIARIO': {'type': 'integer'},
    'DESCRICAO_GENERO': {'type': 'string'},
    'CODIGO_ESTADO_CIVIL': {'type': 'string'},
    'DESCRICAO_PARENTESCO': {'type': 'string'},
    'TIPO_PLANO': {'type': 'string'},
    'TIPO_ACOMODACAO_PLANO': {'type': 'string'},
    'DATA_ADMISSAO': {"type": "date", "format": "dd/mm/yyyy"},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "dd/mm/yyyy"},
    'DATA_REFERENCIA': {"type": "date", "format": "dd/mm/yyyy"},
    'VALOR_PAGO': {'type': 'money'},
    'VALOR_ACERTO': {'type': 'money'},
    'VALOR_FRANQUIA': {'type': 'money'},
    'VALOR_COPARTICIPACAO': {'type': 'money'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}

    }

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_UNIMED_BH', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED_BH'
table_name = 'UNIMED_BH_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'DATA_REFERENCIA': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'NOME_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'CODIGO_CONTRATO': {'type': 'string'},
    'TIPO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'DESCRICAO_TIPO_CONTRATO': {'type': 'string'},
    'DESCRICAO_TIPO_CONTRATACAO': {'type': 'string'},
    'CODIGO_ID_PESSOA': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'DESCRICAO_GENERO': {'type': 'string'},
    'DATA_INICIO_VIGENCIA': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'DESCRICAO_PARENTESCO': {'type': 'string'},
    'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
    'CODIGO_MATRICULA': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'CODIGO_AUTORIZACAO': {'type': 'string'},
    'DATA_PROCEDIMENTO': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'DESCRICAO_TIPO_SINISTRO': {'type': 'string'},
    'DATA_INTERNACAO': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'DATA_ALTA_PROCEDIMENTO': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
    'DESCRICAO_MOTIVO_ALTA': {'type': 'string'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type': 'string'},
    'NOME_PRESTADOR': {'type': 'string'},
    'INDICA_PRESTADOR_PROPRIO': {'type': 'string'},
    'DESCRICAO_ESPECIALIDADE_PRESTADOR': {'type': 'string'},
    'CODIGO_PROCEDIMENTO': {'type': 'string'},
    'DESCRICAO_PROCEDIMENTO': {'type': 'string'},
    'CODIGO_CAPITULO_TABELA_AMB': {'type': 'string'},
    'DESCRICAO_CAPITULO_TABELA_AMB': {'type': 'string'},
    'CODIGO_CID': {'type': 'string'},
    'DESCRICAO_CID': {'type': 'string'},
    'DESCRICAO_TIPO_CONSULTA': {'type': 'string'},
    'DESCRICAO_GRUPO_SINISTRO': {'type': 'string'},
    'DESCRICAO_SUB_GRUPO_SINISTRO': {'type': 'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type': 'integer'},
    'VALOR_PAGO': {'type': 'money'},
    'CODIGO_CNPJ_CONTRATO': {'type': 'string'},
    'CODIGO_CPF': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}

    }

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_UNIMED_BH', schema='DATABRICKS_SILVER', mode='Overwrite')
