# Databricks notebook source
from modules.staging.data_type_transformations import Table
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # PORTO SEGURO

# COMMAND ----------

# MAGIC %md
# MAGIC ## PREMIO

# COMMAND ----------

carrier = 'PORTO_SEGURO'
table_name = 'PORTO_SEGURO_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

porto_seguro_premio_config = {
'CODIGO_SUSEP': {'type': 'string'},
'NOME_CORRETOR': {'type': 'string'},
'CODIGO_CONTRATO': {'type': 'string'},
'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
'NOME_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
'CODIGO_MATRICULA': {'type': 'string'},
'CODIGO_MATRICULA_ESPECIAL': {'type': 'string'},
'CODIGO_CARTEIRINHA_TITULAR': {'type': 'string'},
'NOME_TITULAR': {'type': 'string'},
'DESCRICAO_CARGO': {'type': 'string'},
'EXAME_ADMISSIONAL': {'type': 'string'},
'CODIGO_DEPENDENCIA': {'type': 'string'},
'NOME_BENEFICIARIO': {'type': 'string'},
'CODIGO_CPF': {'type': 'string'},
'ANO_NASCIMENTO': {'type': 'string'},
'IDADE_BENEFICIARIO': {'type': 'integer'},
'DESCRICAO_GENERO': {'type': 'string'},
'DESCRICAO_ESTADO_CIVIL': {'type': 'string'},
'DESCRICAO_PARENTESCO': {'type': 'string'},
'CODIGO_PLANO': {'type': 'string'},
'DESCRICAO_PLANO': {'type': 'string'},
'DATA_INCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy"},
'DATA_EXCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy"},
'DATA_REFERENCIA': {"type": "date", "format": "dd/MM/yyyy"},
'VALOR_FATURAMENTO_BRUTO': {'type': 'money'},
'BONUS_PREMIO': {'type': 'string'},
'VALOR_DESCONTO': {'type': 'money'},
'VALOR_PAGO': {'type': 'money'},
'VALOR_COPARTICIPACAO': {'type': 'money'},
'INDICA_UTILIZACAO_INDEVIDA': {'type': 'string'},
'NOME_OPERADORA_ARQUIVO': {'type': 'string'},
'ETL_SOURCE_FILE_NAME': {'type': 'string'},
'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
'RUN_ID': {'type': 'string'},
'DATE_TIME': {'type': 'timestamp'}
}

table.column_cast(porto_seguro_premio_config)
table.upload_table_to_snowflake('PREMIO_PORTO_SEGURO')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------

carrier = 'PORTO_SEGURO'
table_name = 'PORTO_SEGURO_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

porto_seguro_sinistro_config = {
'CODIGO_CONTRATO': {'type': 'string'},
'NOME_CONTRATO': {'type': 'string'},
'CODIGO_SUBCONTRATO': {'type': 'string'},
'NOME_SUBCONTRATO': {'type': 'string'},
'UNIDADE_MEDIDA': {'type': 'string'},
'DESCRICAO_UNIDADE_MEDIDA': {'type': 'string'},
'DATA_REFERENCIA': {"type": "date", "format": "MM/yyyy"},
'CODIGO_PLANO': {'type': 'string'},
'DESCRICAO_PLANO': {'type': 'string'},
'CODIGO_DEPENDENCIA': {'type': 'string'},
'NOME_BENEFICIARIO': {'type': 'string'},
'DATA_INCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy"},
'DATA_EXCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy"},
'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
'DESCRICAO_GENERO': {'type': 'string'},
'DESCRICAO_PARENTESCO': {'type': 'string'},
'NUMERO_CONTA_OPERADORA': {'type': 'string'},
'CODIGO_INSTALACAO': {'type': 'string'},
'DESCRICAO_INSTALACAO': {'type': 'string'},
'CODIGO_PRESTADOR': {'type': 'string'},
'NOME_PRESTADOR': {'type': 'string'},
'TIPO_ATENDIMENTO': {'type': 'string'},
'DATA_PROCEDIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
'CODIGO_CID': {'type': 'string'},
'DESCRICAO_CID': {'type': 'string'},
'CODIGO_AUTORIZACAO': {'type': 'string'},
'QUANTIDADE_PROCEDIMENTO': {'type': 'integer'},
'VALOR_PAGO': {'type': 'money'},
'CODIGO_EVENTO': {'type': 'string'},
'DESCRICAO_EVENTO': {'type': 'string'},
'CODIGO_SUBGRUPO': {'type': 'string'},
'DESCRICAO_SUBGRUPO': {'type': 'string'},
'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
'DESCRICAO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
'TIPO_SERVICO': {'type': 'string'},
'ETL_SOURCE_FILE_NAME': {'type': 'string'},
'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
'RUN_ID': {'type': 'string'},
'DATE_TIME': {'type': 'timestamp'}
}

table.column_cast(porto_seguro_sinistro_config)
table.upload_table_to_snowflake('SINISTRO_PORTO_SEGURO')
