# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # OMINT

# COMMAND ----------

carrier = 'OMINT'
table_name = 'OMINT_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")


config = {
    'DATA_REFERENCIA': {'type': 'date',  'format': 'yyyy-mm-dd'},
    'DESCRICAO_GENERO': {'type':'string'},
    'FAIXA_ETARIA_OPERADORA': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'NOME_CONTRATO': {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type':'string'},
    'NOME_EMPRESA_OPERADORA': {'type':'string'},
    'DESCRICAO_TIPO_OPERACAO': {'type':'string'},
    'CODIGO_PLANO': {'type':'string'},
    'QUANTIDADE_BENEFICIARIO_ATIVO': {'type':'integer'},
    'VALOR_PAGO': {'type':'money'},
    'VALOR_SINISTRO': {'type':'money'},
    'VALOR_COPARTICIPACAO': {'type':'money'},
    'CODIGO_MATRICULA': {'type':'integer'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}


table.column_cast(config)



table.upload_table_to_snowflake('PREMIO_OMINT', schema='DATABRICKS_SILVER', mode ='Overwrite')

# COMMAND ----------



carrier = 'OMINT'
table_name = 'OMINT_SINISTRO'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'CODIGO_CONTRATO': {'type':'string'},
    'NOME_CONTRATO': {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type':'string'},
    'NOME_EMPRESA_OPERADORA': {'type':'string'},
    'DESCRICAO_GENERO': {'type':'string'},
    'CODIGO_PLANO': {'type':'string'},
    'DESCRICAO_TIPO_OPERACAO': {'type':'string'},
    'NOME_PRESTADOR': {'type':'string'},
    'DATA_PROCEDIMENTO':  {'type': 'date',  'format': 'yyyy-mm-dd'},
    'DATA_ALTA_PROCEDIMENTO': {'type': 'date',  'format': 'yyyy-mm-dd'},
    'DATA_REFERENCIA':  {'type': 'date',  'format': 'yyyy-mm-dd'},
    'CODIGO_PROCEDIMENTO': {'type':'string'},
    'DESCRICAO_PROCEDIMENTO': {'type':'string'},
    'TIPO_ATENDIMENTO': {'type':'string'},
    'CODIGO_AUTORIZACAO': {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type':'integer'},
    'VALOR_PAGO': {'type':'money'},
    'VALOR_COPARTICIPACAO': {'type':'money'},
    'FAIXA_ETARIA_OPERADORA': {'type':'string'},
    'DESCRICAO_PARENTESCO': {'type':'string'},
    'CODIGO_MATRICULA': {'type':'string'},
    'DESCRICAO_TIPO_SINISTRO': {'type':'string'},
    'INDICA_BENEFICIARIO_LEI_ARTIGO_30_31': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},    
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}

}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_OMINT', schema='DATABRICKS_SILVER', mode='Overwrite')
