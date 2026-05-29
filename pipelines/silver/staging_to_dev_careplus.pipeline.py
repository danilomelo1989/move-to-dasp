# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # CAREPLUS

# COMMAND ----------

# MAGIC %md
# MAGIC ## PREMIO

# COMMAND ----------

carrier = 'CAREPLUS'
table_name = 'CAREPLUS_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
'CODIGO_CONTRATO': {'type': 'string'},
'NOME_CONTRATO': {'type': 'string'},
'DATA_REFERENCIA': {"type": "date", "format": "MM/yyyy"},
'CODIGO_PLANO': {'type': 'string'},
'DESCRICAO_TIPO_ABRANGENCIA_PLANO': {'type': 'string'},
'DESCRICAO_GENERO': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_0_A_17': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_18_A_29': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_30_A_39': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_40_A_49': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_50_A_59': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_60_A_69': {'type': 'string'},
'FAIXA_ETARIA_OPERADORA_MAIOR_70': {'type': 'string'},
'VALOR_PAGO': {'type': 'money'},
'CODIGO_EMPRESA_OPERADORA': {'type': 'string'},
'DESCRICAO_PLANO': {'type': 'string'},
'ETL_SOURCE_FILE_NAME' : {'type':'string'},
'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
'RUN_ID' : {'type':'string'},
'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
}

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_CAREPLUS', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------

carrier = 'CAREPLUS'
table_name = 'CAREPLUS_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
'NOME_OPERADORA_ARQUIVO': {'type': 'string'},
'CODIGO_CONTRATO': {'type': 'string'},
'CODIGO_BENEFICIARIO': {'type': 'string'},
'DATA_PROCEDIMENTO': {"type": "date", "format": "yyyyMMdd"},
'NOME_OPERADORA_ARQUIVO_REPETIDO': {'type': 'string'},
'DATA_RECONHECIMENTO_CONTA': {"type": "date", "format": "yyyyMMdd"},
'CODIGO_PRESTADOR': {'type': 'string'},
'NOME_PRESTADOR': {'type': 'string'},
'CODIGO_PROCEDIMENTO': {'type': 'string'},
'DESCRICAO_PROCEDIMENTO': {'type': 'string'},
'CODIGO_TITULAR': {'type': 'string'},
'TIPO_ATENDIMENTO': {'type': 'string'},
'VALOR_PAGO': {'type': 'money'},
'QUANTIDADE_PROCEDIMENTO': {'type': 'integer'},
'DESCRICAO_GRUPO_SINISTRO': {'type': 'string'},
'NOME_LOCAL_ATENDIMENTO': {'type': 'string'},
'NUMERO_CRM_MEDICO_SOLICITANTE': {'type': 'string'},
'CODIGO_GRUPO_ECONOMICO_EMPRESA': {'type': 'string'},
'ETL_SOURCE_FILE_NAME' : {'type':'string'},
'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
'RUN_ID' : {'type':'string'},
'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
}

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_CAREPLUS', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CADASTRO

# COMMAND ----------

carrier = 'CAREPLUS'
table_name = 'CAREPLUS_CADASTRO'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
'NOME_OPERADORA_ARQUIVO': {'type': 'string'},
'NOME_CONTRATO': {'type': 'string'},
'CODIGO_CONTRATO': {'type': 'string'},
'DATA_INICIO_VIGENCIA_CONTRATO': {"type": "date", "format": "MM/yyyy"},
'CODIGO_BENEFICIARIO': {'type': 'string'},
'NOME_BENEFICIARIO': {'type': 'string'},
'DATA_NASCIMENTO': {"type": "date", "format": "yyyyMMdd"},
'DATA_INICIO_VIGENCIA': {"type": "date", "format": "yyyyMMdd"},
'DATA_FIM_VIGENCIA': {"type": "date", "format": "yyyyMMdd"},
'CODIGO_CPF': {'type': 'string'},
'NUMERO_PIS': {'type': 'string'},
'NUMERO_IDENTIDADE': {'type': 'string'},
'CODIGO_INSTITUICAO_EMISSOR': {'type': 'string'},
'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
'CODIGO_TITULAR': {'type': 'string'},
'DESCRICAO_ESTADO_CIVIL': {'type': 'string'},
'DESCRICAO_GENERO': {'type': 'string'},
'NOME_MAE': {'type': 'string'},
'NUMERO_TELEFONE_BENEFICIARIO': {'type': 'string'},
'DESCRICAO_ESTADO_BENEFICIARIO': {'type': 'string'},
'CODIGO_MATRICULA': {'type': 'string'},
'DESCRICAO_CIDADE_BENEFICIARIO': {'type': 'string'},
'UF_BENEFICIARIO': {'type': 'string'},
'DESCRICAO_ENDERECO_BENEFICIARIO': {'type': 'string'},
'DATA_ULTIMA_SITUACAO_BENEFICIARIO': {"type": "date", "format": "yyyyMMdd"},
'DESCRICAO_PARENTESCO': {'type': 'string'},
'CODIGO_PARENTESCO': {'type': 'string'},
'CODIGO_PLANO': {'type': 'string'},
'DESCRICAO_PLANO': {'type': 'string'},
'CODIGO_TRANSFERENCIA_BENEFICIARIO': {'type': 'string'},
'EMAIL_BENEFICIARIO': {'type': 'string'},
'ETL_SOURCE_FILE_NAME' : {'type':'string'},
'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
'RUN_ID' : {'type':'string'},
'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}

}

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_CAREPLUS', schema='DATABRICKS_SILVER', mode='Overwrite')
