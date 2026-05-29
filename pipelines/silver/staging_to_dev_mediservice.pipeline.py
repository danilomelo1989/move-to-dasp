# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
#from pyspark.sql.expressions import Window
#from pyspark.sql.functions import *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # MEDISERVICE

# COMMAND ----------

# MAGIC %md
# MAGIC ## CADASTRO

# COMMAND ----------

carrier = 'MEDISERVICE'
table_name = 'MEDISERVICE_CADASTRO'
file_type = 'CADASTRO'

table = Table(table_name)
datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

mediservice_cadastro_config = {
    'DATA_REFERENCIA': {'type':'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type':'integer'},
    'NOME_GRUPO_ECONOMICO_OPERADORA': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'NOME_CONTRATO': {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type':'integer'},
    'NOME_EMPRESA_OPERADORA': {'type':'string'},
    'CODIGO_BENEFICIARIO': {'type':'string'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'NOME_ELEGIBILIDADE': {'type':'string'},
    'CODIGO_MATRICULA': {'type':'string'},
    'CODIGO_CARTEIRINHA': {'type':'string'},
    'CODIGO_PLANO': {'type':'integer'},
    'DESCRICAO_PLANO': {'type':'string'},
    'DESCRICAO_PARENTESCO': {'type':'string'},
    'DESCRICAO_GENERO': {'type':'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'CODIGO_CPF': {'type':'string'},
    'DESCRICAO_ESTADO_CIVIL': {'type':'string'},
    'DATA_INICIO_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'ANO_REFERENCIA': {'type':'string'},
    'CODIGO_ESTADO_CIVIL': {'type':'integer'},
    'CODIGO_PARENTESCO':{'type':'integer'},
    'CODIGO_GENERO': {'type':'string'},
    'CIDADE_BENEFICIARIO': {'type':'string'},
    'FAIXA_ETARIA_OPERADORA': {'type':'string'},
    'IDADE_BENEFICIARIO': {'type':'integer'},
    'MES_REFERENCIA': {'type':'string'},
    'CODIGO_UF': {'type':'string'},
    'DATA_EXCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy"},
    'QUANTIDADE_BENEFICIARIO_ATIVO': {'type':'string'},
    'DATA_INCLUSAO_PLANO': {"type": "date", "format": "dd/MM/yyyy"},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'DENSE_RANK': {'type':'integer'}
}


table.column_cast(mediservice_cadastro_config)



table.upload_table_to_snowflake('CADASTRO_MEDISERVICE', schema='DATABRICKS_SILVER', mode ='Overwrite')


# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------

carrier = 'MEDISERVICE'
table_name = 'MEDISERVICE_SINISTRO'
file_type = 'SINISTRO'

 
table = Table(table_name)


datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

table.filter_table("TRIM(DATA_PROCEDIMENTO) != 'Data Atendimento'")
 
config = {
    'DATA_PROCEDIMENTO': {'type': 'date', 'rename': 'DATA_PROCEDIMENTO', 'format': 'dd/MM/yyyy'},
    'DATA_ALTA_PROCEDIMENTO': {'type': 'date', 'rename': 'DATA_ALTA_PROCEDIMENTO', 'format': 'dd/MM/yyyy'},
    'DATA_REFERENCIA': {'type': 'date', 'rename': 'DATA_REFERENCIA', 'format': 'dd/MM/yyyy'},
    'DESCRICAO_ESPECIALIDADE': {'type': 'string', 'rename': 'DESCRICAO_ESPECIALIDADE'},
    'DESCRICAO_PARENTESCO': {'type': 'string', 'rename': 'DESCRICAO_PARENTESCO'},
    'DESCRICAO_TIPO_SINISTRO': {'type': 'string', 'rename': 'DESCRICAO_TIPO_SINISTRO'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type': 'string', 'rename': 'DESCRICAO_SUB_TIPO_SINISTRO'},
    'CODIGO_PROCEDIMENTO': {'type': 'string', 'rename': 'CODIGO_PROCEDIMENTO'},
    'DESCRICAO_PROCEDIMENTO': {'type': 'string', 'rename': 'DESCRICAO_PROCEDIMENTO'},
    'CODIGO_PLANO': {'type': 'string', 'rename': 'CODIGO_PLANO'},
    'DESCRICAO_GENERO': {'type': 'string', 'rename': 'DESCRICAO_GENERO'},
    'DATA_NASCIMENTO': {'type': 'date', 'rename': 'DATA_NASCIMENTO', 'format': 'dd/MM/yyyy'},
    'IDADE_BENEFICIARIO': {'type': 'string', 'rename': 'IDADE_BENEFICIARIO'},
    'CODIGO_MATRICULA': {'type': 'string', 'rename': 'CODIGO_MATRICULA'},
    'NOME_CONTRATO': {'type': 'string', 'rename': 'NOME_CONTRATO'},
    'NOME_LOCAL_EMPRESA_OPERADORA': {'type': 'string', 'rename': 'NOME_LOCAL_EMPRESA_OPERADORA'},
    'NOME_GRUPO_ECONOMICO_OPERADORA': {'type': 'string', 'rename': 'NOME_GRUPO_ECONOMICO_OPERADORA'},
    'NOME_BENEFICIARIO': {'type': 'string', 'rename': 'NOME_BENEFICIARIO'},
    'NOME_PRESTADOR': {'type': 'string', 'rename': 'NOME_PRESTADOR'},
    'CODIGO_PRESTADOR': {'type': 'string', 'rename': 'CODIGO_PRESTADOR'},
    'NOME_TITULAR': {'type': 'string', 'rename': 'NOME_TITULAR'},
    'CODIGO_CARTEIRINHA': {'type': 'string', 'rename': 'CODIGO_CARTEIRINHA'},
    'TIPO_ATENDIMENTO': {'type': 'string', 'rename': 'TIPO_ATENDIMENTO'},
    'UF_BENEFICIARIO': {'type': 'string', 'rename': 'UF_BENEFICIARIO'},
    'NOME_LOCAL_INTERNO_EMPRESA': {'type': 'string', 'rename': 'NOME_LOCAL_INTERNO_EMPRESA'},
    'ANO_REFERENCIA': {'type': 'string', 'rename': 'ANO_REFERENCIA'},
    'ANO_MES_REFERENCIA': {'type': 'string', 'rename': 'ANO_MES_REFERENCIA'},
    'VALOR_INSS_ISS': {'type': 'money', 'rename': 'VALOR_INSS_ISS'},
    'VALOR_PAGO': {'type': 'money', 'rename': 'VALOR_PAGO'},
    'QUANTIDADE_PROCEDIMENTO': {'type': 'integer', 'rename': 'QUANTIDADE_PROCEDIMENTO'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string', 'rename': 'CODIGO_GRUPO_ECONOMICO_OPERADORA'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA': {'type': 'string', 'rename': 'CODIGO_LOCAL_EMPRESA_OPERADORA'},
    'CODIGO_EMPRESA_OPERADORA': {'type': 'string', 'rename': 'CODIGO_EMPRESA_OPERADORA'},
    'CODIGO_CONTRATO': {'type': 'string', 'rename': 'CODIGO_CONTRATO'},
    'VALOR_COPARTICIPACAO': {'type': 'money', 'rename': 'VALOR_COPARTICIPACAO'},
    'DESCRICAO_TIPO_INTERNACAO': {'type': 'string', 'rename': 'DESCRICAO_TIPO_INTERNACAO'},
    'CODIGO_AUTORIZACAO': {'type': 'string', 'rename': 'CODIGO_AUTORIZACAO'},
    'VALOR_PAGO_REEMBOLSO': {'type': 'money', 'rename': 'VALOR_PAGO_REEMBOLSO'},
    'VALOR_EMPRESA': {'type': 'money', 'rename': 'VALOR_EMPRESA'},
    'NUMERO_CONTA_OPERADORA': {'type': 'string', 'rename': 'NUMERO_CONTA_OPERADORA'},
    'NUMERO_SISTEMA_OPERADORA': {'type': 'string', 'rename': 'NUMERO_SISTEMA_OPERADORA'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string', 'rename': 'ETL_SOURCE_FILE_NAME'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string', 'rename': 'ETL_SOURCE_ZIP_NAME'},
    'RUN_ID': {'type': 'string', 'rename': 'RUN_ID'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'DENSE_RANK': {'type':'integer'}

}

 
table.column_cast(config)


 

 
table.upload_table_to_snowflake('SINISTRO_MEDISERVICE', schema='DATABRICKS_SILVER', mode='Overwrite')
