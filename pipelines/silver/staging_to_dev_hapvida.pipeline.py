# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # HAPVIDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## PREMIO

# COMMAND ----------

carrier = 'HAPVIDA'
table_name = 'HAPVIDA_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

hapvida_premio_config = {
    'CODIGO_CONTRATO': {'type': 'string'},
    'DATA_REFERENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'ANO_MES_REFERENCIA': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'VALOR_PAGO': {'type': 'money'},
    'VALOR_PAGO_ODONTO': {'type': 'money'},
    'VALOR_TAXA_ADESAO': {'type': 'money'},
    'VALOR_ADICIONAL': {'type': 'money'},
    'VALOR_ADICIONAL_IDADE': {'type': 'money'},
    'VALOR_SERVICO': {'type': 'money'},
    'VALOR_COPARTICIPACAO': {'type': 'money'},
    'VALOR_FATURAMENTO_EXTRA': {'type': 'money'},
    'VALOR_DESCONTO': {'type': 'money'},
    'VALOR_DESCONTO_FATURAMENTO': {'type': 'money'},
    'NOME_EMPRESA_OPERADORA': {'type': 'string'},
    'NOME_SUB_EMPRESA': {'type': 'string'},
    'DESCRICAO_BOLETO': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}
}

table.column_cast(hapvida_premio_config)
table.upload_table_to_snowflake('PREMIO_HAPVIDA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------

carrier = 'HAPVIDA'
table_name = 'HAPVIDA_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

hapvida_sinistro_config = {
    'CODIGO_CONTRATO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'NUMERO_CONTA_OPERADORA': {'type': 'string'},
    'NUMERO_GUIA_CONTA': {'type': 'string'},
    'DATA_REFERENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'ANO_MES_REFERENCIA': {'type': 'string'},
    'DATA_PROCEDIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'CODIGO_PRESTADOR': {'type': 'string'},
    'NOME_PRESTADOR': {'type': 'string'},
    'TIPO_ACOMODACAO_PLANO': {'type': 'string'},
    'CODIGO_PROCEDIMENTO': {'type': 'string'},
    'DESCRICAO_PROCEDIMENTO': {'type': 'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type': 'integer'},
    'VALOR_PAGO': {'type': 'money'},
    'CLASSIFICACAO': {'type': 'string'},
    'DESCRICAO_TIPO_SINISTRO': {'type': 'string'},
    'CODIGO_AUTORIZACAO': {'type': 'string'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type': 'string'},
    'DESCRICAO_ESPECIALIDADE': {'type': 'string'},
    'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
    'IDADE_BENEFICIARIO': {'type': 'integer'},
    'DESCRICAO_GENERO': {'type': 'string'},
    'DESCRICAO_CIDADE_BENEFICIARIO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'VALOR_AMBULATORIO': {'type': 'money'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}

}

table.column_cast(hapvida_sinistro_config)
table.upload_table_to_snowflake('SINISTRO_HAPVIDA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'HAPVIDA'
table_name = 'HAPVIDA_CADASTRO'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

hapvida_cadastro_config = {
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'CODIGO_TITULAR': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'CODIGO_FAMILIA': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'CODIGO_MATRICULA': {'type': 'string'},
    'CODIGO_CPF': {'type': 'string'},
    'CODIGO_SITUACAO_BENEFICIARIO': {'type': 'string'},
    'CODIGO_CONTRATO': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type': 'string'},
    'NOME_EMPRESA_OPERADORA': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'DESCRICAO_GENERO': {'type': 'string'},
    'DATA_INICIO_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'DESCRICAO_CIDADE_BENEFICIARIO': {'type': 'string'},
    'UF_BENEFICIARIO': {'type': 'string'},
    'NUMERO_REGISTRO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'IDADE_BENEFICIARIO': {'type': 'integer'},
    'DESCRICAO_ELEGIBILIDADE': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME': {'type': 'string'},
    'ETL_SOURCE_ZIP_NAME': {'type': 'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

table.column_cast(hapvida_cadastro_config)
table.upload_table_to_snowflake('CADASTRO_HAPVIDA', schema='DATABRICKS_SILVER', mode='Overwrite')
