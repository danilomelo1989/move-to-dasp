# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # SEGUROS-UNIMED

# COMMAND ----------

# MAGIC %md
# MAGIC ## PREMIO

# COMMAND ----------

carrier = 'SEGUROS_UNIMED'
table_name = 'SEGUROS_UNIMED_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)
datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

table.filter_table("ETL_SOURCE_FILE_NAME != 'PRE_2218_012023_1664_LINDT.txt'")
table.filter_table("ETL_SOURCE_FILE_NAME != 'PRE_2218_022023_1664_LINDT.txt'")

seguros_unimed_premio_config = {
    'DATE_TIME': {'type': 'timestamp'},
    'RUN_ID': {'type': 'string'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'CODIGO_CONTRATO': {'type': 'string'},
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
    'DATA_NASCIMENTO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'IDADE_BENEFICIARIO': {"type": "integer"},
    'DESCRICAO_GENERO': {'type': 'string'},
    'CODIGO_ESTADO_CIVIL': {'type': 'string'},
    'DESCRICAO_PARENTESCO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'DATA_ADMISSAO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'DATA_PAGAMENTO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'VALOR_PAGO_LIQUIDO': {'type': 'money'},
    'VALOR_PAGO': {'type': 'money'},
    'VALOR_ACERTO': {'type': 'money'},
    'VALOR_FRANQUIA': {'type': 'money'},
    'VALOR_COPARTICIPACAO': {'type': 'money'},
    'CODIGO_ELEGIBILIDADE': {'type': 'string'},
    'DATA_REFERENCIA': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'NUMERO_LANCAMENTO_TS': {'type': 'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}    
}

table.column_cast(seguros_unimed_premio_config)
table.upload_table_to_snowflake('PREMIO_SEGUROS_UNIMED', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------

carrier = 'SEGUROS_UNIMED'
table_name = 'SEGUROS_UNIMED_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)
datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
seguros_unimed_premio_config = {
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'CODIGO_CONTRATO': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
    'CODIGO_MATRICULA': {'type': 'string'},
    'CODIGO_MATRICULA_ESPECIAL': {'type': 'string'},
    'CODIGO_TITULAR': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'DESCRICAO_PARENTESCO': {'type': 'string'},
    'INDICA_EXAME_ADMISSIONAL': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'IDADE_BENEFICIARIO': {'type': 'string'},
    'DESCRICAO_GENERO': {'type': 'string'},
    'DATA_PROCEDIMENTO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'INDICA_INTERNACAO': {'type': 'string'},
    'DATA_INTERNACAO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'CODIGO_TIPO_SINISTRO': {'type': 'string'},
    'DESCRICAO_TIPO_SINISTRO': {'type': 'string'},
    'VALOR_PAGO': {'type': 'money'},
    'DATA_PAGAMENTO': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'QUANTIDADE_PROCEDIMENTO': {'type': 'string'},
    'VALOR_COPARTICIPACAO': {'type': 'money'},
    'CODIGO_PRESTADOR': {'type': 'string'},
    'NOME_PRESTADOR': {'type': 'string'},
    'NOME_PRESTADOR_RAZAO_SOCIAL': {'type': 'string'},
    'DESCRICAO_CIDADE_PRESTADOR': {'type': 'string'},
    'TIPO_ATENDIMENTO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'DESCRICAO_ESPECIALIDADE': {'type': 'string'},
    'CODIGO_PROCEDIMENTO': {'type': 'string'},
    'DESCRICAO_PROCEDIMENTO': {'type': 'string'},
    'DESCRICAO_GRUPO_SINISTRO': {'type': 'string'},
    'CODIGO_AUTORIZACAO': {'type': 'string'},
    'DATA_REFERENCIA': {"type": "date", "format": ["dd/MM/yyyy", "MM/yyyy", "dd/MM/yy"]},
    'INDICA_RECUPERACAO_SINISTRO': {'type': 'string'},
    'CODIGO_CORRETOR': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
    'RUN_ID': {'type': 'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
}

table.column_cast(seguros_unimed_premio_config)
table.upload_table_to_snowflake('SINISTRO_SEGUROS_UNIMED', schema='DATABRICKS_SILVER', mode='Overwrite')
