# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # ONE_HEALTH

# COMMAND ----------

carrier = 'ONE-HEALTH'
table_name = 'ONE_HEALTH_PARTICIPACAO_1'
file_type = 'PARTICIPACAO'

table = Table(table_name)
datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

one_health_participacao_1_config = {

    'TIPO_REGISTRO': {'type':'string'},
    'TIPO_ARQUIVO': {'type':'string'},
    'COMPETENCIA': {'type':'string'},
    'NOME_EMPRESA_OPERADORA': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}
}


table.column_cast(one_health_participacao_1_config)





table.upload_table_to_snowflake('PARTICIPACAO_1_ONE_HEALTH', schema='DATABRICKS_SILVER', mode ='Overwrite')


# COMMAND ----------

carrier = 'ONE-HEALTH'
table_name = 'ONE_HEALTH_PARTICIPACAO_2'
file_type = 'PARTICIPACAO'

table = Table(table_name)
datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

one_health_participacao_2_config = {

    'TIPO_REGISTRO': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'CODIGO_MATRICULA': {'type':'integer'},
    'CODIGO_EMPRESA_OPERADORA': {'type':'integer'},
    'DESCRICAO_ELEGIBILIDADE': {'type':'string'},
    'CODIGO_CARTEIRINHA': {'type':'integer'},
    'CODIGO_CARTEIRINHA_TITULAR': {'type':'integer'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'CODIGO_PRESTADOR': {'type':'string'},
    'DESCRICAO_PRESTADOR': {'type':'string'},
    'DESCRICAO_PROCEDIMENTO': {'type':'string'},
    'VALOR_COPARTICIPACAO': {'type':'money'},
    'CODIGO_TIPO_SINISTRO': {'type':'string'},
    'DATA_PROCEDIMENTO': {'type': 'date', 'format': 'yyyymmdd'},
    'DESCRICAO_ESPECIALIDADE': {'type':'string'},
    'DATA_EXCLUSAO_PLANO':  {'type': 'date', 'format': 'dd/MM/yyyy'},
    'DATA_REFERENCIA': {'type':'string'},
    'CODIGO_TIPO_PRESTADOR': {'type':'string'},
    'DESCRICAO_SINISTRO_OPERADORA': {'type':'string'},
    'CODIGO_PROCEDIMENTO': {'type':'integer'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}


table.column_cast(one_health_participacao_2_config)




table.upload_table_to_snowflake('PARTICIPACAO_2_ONE_HEALTH', schema='DATABRICKS_SILVER', mode ='Overwrite')

# COMMAND ----------

carrier = 'ONE-HEALTH'
table_name = 'ONE_HEALTH_PARTICIPACAO_3'
file_type = 'PARTICIPACAO'

table = Table(table_name)
datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

one_health_participacao_3_config = {

    'TIPO_REGISTRO': {'type':'string'},
    'TOTAL_REGISTROS': {'type':'integer'},
    'VALOR_TOTAL': {'type':'money'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},    
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}


table.column_cast(one_health_participacao_3_config)



table.upload_table_to_snowflake('PARTICIPACAO_3_ONE_HEALTH', schema='DATABRICKS_SILVER', mode ='Overwrite')

# COMMAND ----------

carrier = 'ONE-HEALTH'
table_name = 'ONE_HEALTH_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)
table.filter_table("UPPER(TRIM(FILIAL)) != 'FILIAL DA AMIL'")

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
#TODO: negative values on field VALOR_PAGO

one_health_premio_config = {

    'FILIAL': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'CODIGO_EMPRESA_OPERADORA': {'type':'integer'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA': {'type':'string'},
    'LOTACAO': {'type':'string'},
    'CODIGO_FAMILIA': {'type':'integer'},
    'CODIGO_TIPO_BENEFICIARIO': {'type':'integer'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'CODIGO_PLANO': {'type':'string'},
    'DESCRICAO_PLANO': {'type':'string'},
    'DATA_NASCIMENTO': {'type': 'date',  'format': 'yyyymmdd'},
    'DATA_INCLUSAO_PLANO': {'type': 'date', 'format': 'yyyymmdd'},
    'DESCRICAO_ESTADO_CIVIL': {'type':'string'},
    'DESCRICAO_GENERO': {'type':'string'},
    'CODIGO_PARENTESCO': {'type':'string'},
    'VALOR_PAGO': {'type':'money'},
    'VALOR_ADITIVO': {'type':'money'},
    'VALOR_ADITIVO_RESGATE': {'type':'money'},
    'VALOR_ADITIVO_INTERNACAO': {'type':'money'},
    'VALOR_ADITIVO_MEDICINA_DOMICILIAR': {'type':'money'},
    'VALOR_ADITIVO_MEDICAMENTOS': {'type':'money'},
    'CODIGO_MATRICULA': {'type':'integer'},
    'CODIGO_CARTEIRINHA': {'type':'integer'},
    'CPF': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},    
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}


table.column_cast(one_health_premio_config)



table.upload_table_to_snowflake('PREMIO_ONE_HEALTH', schema='DATABRICKS_SILVER', mode ='Overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------


carrier = 'ONE-HEALTH'
table_name = 'ONE_HEALTH_SINISTRO'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {
    'NOME_OPERADORA_ARQUIVO': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'NOME_CONTRATO': {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type':'integer'},
    'NOME_EMPRESA_OPERADORA': {'type':'string'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA': {'type':'string'},
    'NOME_LOCAL_EMPRESA_OPERADORA': {'type':'string'},
    'CODIGO_MATRICULA': {'type':'integer'},
    'CODIGO_CARTEIRINHA_TITULAR': {'type':'integer'},
    'NOME_TITULAR': {'type':'string'},
    'CODIGO_CPF': {'type':'string'},
    'NOME_CARGO': {'type':'string'},
    'CODIGO_CARTEIRINHA': {'type':'integer'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'DATA_NASCIMENTO': {'type': 'date',  'format': 'dd/MM/yyyy'},
    'DESCRICAO_GENERO': {'type':'string'},
    'NOME_ELEGIBILIDADE': {'type':'string'},
    'CODIGO_PARENTESCO': {'type':'integer'},
    'DESCRICAO_PARENTESCO': {'type':'string'},
    'DESCRICAO_ESTADO_CIVIL': {'type':'string'},
    'CODIGO_PLANO': {'type':'integer'},
    'DESCRICAO_PLANO': {'type':'string'},
    'DESCRICAO_SITUACAO_BENEFICIARIO': {'type':'string'},
    'CIDADE_BENEFICIARIO': {'type':'string'},
    'UF_BENEFICIARIO': {'type':'string'},
    'CODIGO_PROCEDIMENTO': {'type':'integer'},
    'DESCRICAO_PROCEDIMENTO': {'type':'string'},
    'TABELA_PROCEDIMENTO': {'type':'string'},
    'CODIGO_TIPO_SINISTRO': {'type':'string'},
    'CODIGO_ORIGEM_SINISTRO': {'type':'string'},
    'CODIGO_SUB_TIPO_SINISTRO': {'type':'string'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type':'string'},
    'CODIGO_ESPECIALIDADE': {'type':'string'},
    'DESCRICAO_ESPECIALIDADE': {'type':'string'},
    'CODIGO_CID': {'type':'string'},
    'DESCRICAO_CID': {'type':'string'},
    'CODIGO_SINISTRO': {'type':'string'},
    'CODIGO_AUTORIZACAO': {'type':'string'},
    'TIPO_ATENDIMENTO': {'type':'string'},
    'DATA_PROCEDIMENTO': {'type': 'date',  'format': 'dd/MM/yyyy'},
    'DATA_ALTA_PROCEDIMENTO': {'type': 'date',  'format': 'dd/MM/yyyy'},
    'DATA_REFERENCIA':{'type': 'date',  'format': 'dd/MM/yyyy'},
    'CODIGO_PRESTADOR': {'type':'integer'},
    'DESCRICAO_PRESTADOR': {'type':'string'},
    'QUANTIDADE_SINISTRO_CONTA': {'type':'integer'},
    'VALOR_APRESENTADO': {'type':'money'},
    'VALOR_PAGO': {'type':'money'},
    'VALOR_EMPRESA': {'type':'money'},
    'VALOR_COPARTICIPACAO': {'type':'money'},
    'TPO_CONSELHO_MEDICO': {'type':'string'},
    'UF_CONSELHO_MEDICO': {'type':'string'},
    'NUMERO_CRM': {'type':'integer'},
    'TIPO_PRESTADOR': {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type':'integer'},
    'INDICA_RESTITUICAO_GLOSA': {'type':'string'},
    'NUMERO_PROTOCOLO_ANS': {'type':'integer'},
    'NUMERO_REEMBOLSO_ANS': {'type':'integer'},
    'DESCRICAO_CONSELHO_EXECUTANTE_ANS': {'type':'string'},
    'NUMERO_CONSELHO_EXECUTANTE_ANS': {'type':'string'},
    'UNIDADE_MEDIDA': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},    
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}

}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_ONE_HEALTH', schema='DATABRICKS_SILVER', mode='Overwrite')
