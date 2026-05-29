# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # UNIMED CAMPINAS

# COMMAND ----------


carrier = 'UNIMED-CAMPINAS'
table_name = 'UNIMED_CAMPINAS_SINISTRO_0'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'DATA_REFERENCIA': {'type':'integer'},
    'NOME_OPERADORA_ARQUIVO': {'type':'string'},
    'CODIGO_CNPJ_OPERADORA': {'type':'string'},
    'CODIGO_CENTRO_CUSTO': {'type':'string'},
    'CODIGO_TITULAR': {'type':'string'},
    'NOME_TITULAR': {'type':'string'},
    'CODIGO_BENEFICIARIO': {'type':'string'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'DESCRICAO_PARENTESCO': {'type':'string'},
    'DATA_APRESENTACAO_CONTA':  {'type': 'date',  'format': 'ddmmyyyy'},
    'DATA_PROCEDIMENTO': {'type': 'date',  'format': 'ddmmyyyy'},
    'CODIGO_IDENTFICADOR_INTERCAMBIO': {'type':'integer'},
    'CODIGO_IDENTIFICADOR_PRODUCAO_MEDICA': {'type':'integer'},
    'DESCRICAO_TIPO_SINISTRO': {'type':'string'},
    'CODIGO_SOLICITACAO': {'type':'integer'},
    'CODIGO_PROCEDIMENTO': {'type':'integer'},
    'DESCRICAO_PROCEDIMENTO': {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type':'integer'},
    'QUANTIDADE_CHS_AMB': {'type':'integer'},
    'VALOR_PAGO': {'type':'money'},
    'VALOR_TAXA_ADMINISTRATIVA': {'type':'money'},
    'CODIGO_ORIGEM_SINISTRO': {'type':'string'},
    'CODIGO_TIPO_COBERTURA': {'type':'string'},
    'USO_OPERADORA': {'type':'string'},
    'DATA_INTERNACAO':  {'type': 'date',  'format': 'ddmmyyyy'},
    'DATA_ALTA_PROCEDIMENTO':  {'type': 'date',  'format': 'ddmmyyyy'},
    'CODIGO_PRESTADOR_SOLICITANTE': {'type':'integer'},
    'CODIGO_PRESTADOR': {'type':'integer'},
    'NOME_PRESTADOR': {'type':'string'},
    'INDICA_ACIDENTE_TRABALHO': {'type':'string'},
    'DATA_FIM_VIGENCIA': {'type': 'date',  'format': 'ddmmyyyy'},
    'DESCRICAO_TIPO_PRESTADOR': {'type':'string'},
    'CLASSIFICACAO_TIPO_PRESTADOR': {'type':'string'},
    'PORCENTAGEM_HONORARIO': {'type':'money'},
    'CODIGO_ESPECIALIDADE': {'type':'integer'},
    'CODIGO_MATRICULA': {'type':'integer'},
    'NUMERO_PERIODO': {'type':'integer'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'VALOR_COPARTICIPACAO': {'type':'money'},
    'INDICA_FATURAMENTO': {'type':'string'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type':'string'},
    'CODIGO_AUTORIZACAO': {'type':'string'},
    'NOME_LOCAL_ATENDIMENTO': {'type':'string'},
    'TIPO_PRESTADOR': {'type':'string'},
    'DATA_NASCIMENTO': {'type': 'date',  'format': 'ddmmyyyy'},
    'DESCRICAO_GENERO': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_UNIMED_CAMPINAS', schema='DATABRICKS_SILVER', mode='Overwrite')
