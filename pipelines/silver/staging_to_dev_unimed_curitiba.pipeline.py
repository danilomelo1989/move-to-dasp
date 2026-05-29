# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # UNIMED CURITIBA

# COMMAND ----------


carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_CADASTRO'
file_type = 'CADASTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

 
config = {

    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'string'},
    'NOME_CONTRATO': {'type':'string'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA': {'type':'string'},
    'DESCRICAO_LOCAL_EMPRESA_OPERADORA': {'type':'string'},
    'CODIGO_PLANO': {'type':'integer'},
    'DESCRICAO_PLANO': {'type':'string'},
    'CODIGO_TIPO_COBERTURA': {'type':'integer'},
    'DESCRICAO_TIPO_COBERTURA': {'type':'string'},
    'CODIGO_CARTEIRINHA': {'type':'string'},
    'CODIGO_BENEFICIARIO_AFINIDADE': {'type':'string'},
    'CODIGO_BENEFICIARIO': {'type':'string'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'CODIGO_IDENTIFICADOR_BENEFICIARIO': {'type':'string'},
    'CODIGO_CNS': {'type':'string'},
    'CODIGO_FAMILIA': {'type':'integer'},
    'DESCRICAO_PARENTESCO': {'type':'string'},
    'CODIGO_MATRICULA': {'type':'integer'},
    'DATA_NASCIMENTO':  {'type': 'date',  'format': 'yyyy-MM-dd'},
    'DATA_INICIO_VIGENCIA':  {'type': 'date',  'format': 'yyyy-MM-dd'},
    'CODIGO_NASCIDO_VIVO': {'type':'string'},
    'NUMERO_IDENTIDADE': {'type':'string'},
    'CODIGO_INSTITUICAO_EMISSOR': {'type':'string'},
    'DATA_EMISSAO_IDENTIDADE':  {'type': 'date',  'format': 'yyyy-MM-dd'},
    'CODIGO_CPF': {'type':'string'},
    'CODIGO_ESTADO_CIVIL': {'type':'string'},
    'DESCRICAO_GENERO': {'type':'string'},
    'DESCRICAO_ENDERECO_BENEFICIARIO': {'type':'string'},
    'NUMERO_ENDERECO_BENEFICIARIO': {'type':'string'},
    'DESCRICAO_COMPLEMENTO_ENDERECO_BENEFICIARIO': {'type':'string'},
    'BAIRRO_ENDERECO_BENEFICIARIO': {'type':'string'},
    'DESCRICAO_CIDADE_BENEFICIARIO': {'type':'string'},
    'UF_BENEFICIARIO': {'type':'string'},
    'CEP_ENDERECO_BENEFICIARIO': {'type':'string'},
    'NOME_MAE': {'type':'string'},
    'NUMERO_UNIMED': {'type':'string'},
    'DESCRICAO_PRESTADOR_CLASSIFICACAO_BRASILEIRA_OCUPACAO': {'type':'string'},
    'NUMERO_PIS': {'type':'string'},
    'CODIGO_REPASSE_ATENDIMENTO': {'type':'string'},
    'DATA_FIM_VIGENCIA': {'type': 'date',  'format': 'yyyy-MM-dd'},
    'DATA_VALIDADE_PLANO': {'type': 'date',  'format': 'yyyy-MM-dd'},
    'DATA_VALIDADE_CARTEIRINHA': {'type': 'date',  'format': 'yyyy-MM-dd'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'SOURCE_FILE_NAME_LINE_NUMBER': {'type':'integer'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('CADASTRO_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------


carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_GERENCIAL'
file_type = 'GERENCIAL'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'DATA_REFERENCIA': {'type':'string'},
    'QUANTIDADE_BENEFICIARIO_ATIVO': {'type':'integer'},
    'VALOR_PAGO_PREMIO': {'type':'money'},
    'VALOR_PAGO_SINISTRO': {'type':'money'},
    'INDICE_SINISTRALIDADE': {'type':'money'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('GERENCIAL_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_C'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {



    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'CODIGO_CONTRATO': {'type':'integer'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA': {'type':'integer'},
    'NOME_CONTRATO': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_C_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_D'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'CODIGO_BENEFICIARIO': {'type':'string'},
    'NOME_BENEFICIARIO': {'type':'string'},
    'DATA_NASCIMENTO': {'type': 'date',  'format': 'ddmmyyyy'},
    'DESCRICAO_GENERO': {'type':'string'},
    'NUMERO_GUIA_CONTA': {'type':'integer'},
    'DATA_PROCEDIMENTO':  {'type': 'date',  'format': 'ddmmyyyy'},
    'DATA_INTERNACAO':  {'type': 'date',  'format': 'ddmmyyyy'},
    'DATA_ALTA_PROCEDIMENTO':  {'type': 'date',  'format': 'ddmmyyyy'},
    'INDICA_EMERGENCIA': {'type':'string'},
    'HORARIO_ATENDIMENTO': {'type':'string'},
    'TIPO_ACOMODACAO_PLANO': {'type':'string'},
    'CODIGO_GRUPO_SINISTRO': {'type':'string'},
    'DESCRICAO_GRUPO_SINISTRO': {'type':'string'},
    'CODIGO_PROCEDIMENTO': {'type':'integer'},
    'QUANTIDADE_PROCEDIMENTO': {'type':'integer'},
    'NUMERO_CRM': {'type':'integer'},
    'VALOR_PAGO': {'type':'money'},
    'USO_OPERADORA': {'type':'string'},
    'CODIGO_MATRICULA': {'type':'string'},
    'DESCRICAO_TIPO_SINISTRO': {'type':'string'},
    'TIPO_ATENDIMENTO': {'type':'string'},
    'NOME_PRESTADOR': {'type':'string'},
    'DATA_PAGAMENTO': {'type': 'date',  'format': 'ddmmyy'},
    'TABELA_PROCEDIMENTO': {'type':'string'},
    'CODIGO_PRESTADOR': {'type':'string'},
    'DESCRICAO_PROCEDIMENTO': {'type':'string'},
    'CODIGO_CPF': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_D_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_F'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'CODIGO_FAMILIA': {'type':'integer'},
    'NOME_TITULAR': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_F_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_G'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'CODIGO_GRUPO_SINISTRO': {'type':'string'},
    'DESCRICAO_GRUPO_SINISTRO': {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type':'integer'},
    'VALOR_PAGO': {'type':'money_signaled'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_G_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_H'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {


    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'NOME_OPERADORA_ARQUIVO': {'type':'string'},
    'DATA_REFERENCIA': {'type':'string'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_H_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_R'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'VALOR_PAGO_SOMA': {'type':'money'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_R_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'UNIMED-CURITIBA'
table_name = 'UNIMED_CURITIBA_SINISTRO_T'
file_type = 'SINISTRO'

 
table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )
table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
 
config = {

    'NUMERO_SEQUENCIAL_ARQUIVO': {'type':'string'},
    'TIPO_REGISTRO': {'type':'string'},
    'VALOR_PAGO_SOMA': {'type':'money'},
    'ETL_SOURCE_FILE_NAME': {'type':'string'},
    'ETL_SOURCE_ZIP_NAME': {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'}


}

 
table.column_cast(config)

 
table.upload_table_to_snowflake('SINISTRO_T_UNIMED_CURITIBA', schema='DATABRICKS_SILVER', mode='Overwrite')
