# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # SULAMERICA

# COMMAND ----------

carrier = 'SULAMERICA'
table_name = 'SULAMERICA_CADASTRO'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")


config = {
    'CODIGO_CONTRATO' : {'type':'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA' : {'type':'string'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'NOME_LOCAL_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_PRODUTO_OPERADORA' : {'type':'string'},
    'CODIGO_LOTACAO' : {'type':'string'},
    'NOME_LOTACAO' : {'type':'string'},
    'UF_EMPRESA' : {'type':'string'},
    'CODIGO_BENEFICIARIO' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_FAMILIA' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_CLASSIFICACAO_BENEFICIARIO': {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_IDENTIFICADOR_TITULAR' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'CODIGO_CPF_TITULAR' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_CPF_DEPENDENTE' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'DATA_NASCIMENTO'  : {"type": "date", "format": "dd/MM/yyyy"},
    'DESCRICAO_GENERO' : {'type':'string'},
    'CODIGO_PARENTESCO' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'DESCRICAO_PARENTESCO' : {'type':'string'},
    'NOME_MAE' : {'type':'string'},
    'CODIGO_PLANO' : {'type':'string'},
    'DESCRICAO_PLANO' : {'type':'string'},
    'TIPO_ACOMODACAO_PLANO' : {'type':'string'},
    'DATA_INCLUSAO_PLANO' : {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_EXCLUSAO_PLANO' : {"type": "date", "format": "dd/MM/yyyy"},
    'CODIGO_PERMANECNCIA_PLANO' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'DESCRICAO_PERMANECNCIA_PLANO' : {'type':'string'},
    'DESCRICAO_ELEGIBILIDADE' : {'type':'string'},
    'SITUACAO_BENEFICIARIO' : {'type':'string'},
    'INDICA_BENEFICIARIO_REMIDO' : {'type':'string'},
    'DATA_DEMISSAO_APOSENADO' : {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_LIMITE_PERMANENCIA_PLANO' : {"type": "date", "format": "dd/MM/yyyy"},
    'CODIGO_SETOR_BENEFICIARIO_EMPRESA' : {'type':'string'},
    'DESCRICAO_SETOR_BENEFICIARIO_EMPRESA' : {'type':'string'},
    'CODIGO_CNS' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'DATA_REFERENCIA' : {"type": "date", "format": "dd/MM/yyyy"},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_SULAMERICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'SULAMERICA'
table_name = 'SULAMERICA_GERENCIAL'
file_type = 'GERENCIAL'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'DESCRICAO_VALOR' : {'type':'string'},
    'VALOR_MES_1' : {'type':'string'},
    'VALOR_MES_2' : {'type':'string'},
    'VALOR_MES_3' : {'type':'string'},
    'VALOR_MES_4' : {'type':'string'},
    'VALOR_MES_5' : {'type':'string'},
    'VALOR_MES_6' : {'type':'string'},
    'VALOR_MES_7' : {'type':'string'},
    'VALOR_MES_8' : {'type':'string'},
    'VALOR_MES_9' : {'type':'string'},
    'VALOR_MES_10' : {'type':'string'},
    'VALOR_MES_11' : {'type':'string'},
    'VALOR_MES_12' : {'type':'string'},
    'VALOR_TOTALIZADO' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}   

    }

table.column_cast(config)
table.upload_table_to_snowflake('GERENCIAL_SULAMERICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'SULAMERICA'
table_name = 'SULAMERICA_PARTICIPACAO'
file_type = 'PARTICIPACAO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'CODIGO_CONTRATO' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_LOCAL_EMPRESA_OPERADORA' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'VALOR_COPARTICIPACAO' : {'type':'money'},
    'ANO_MES_COPARTICIPACAO' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}   

    }

table.column_cast(config)
table.upload_table_to_snowflake('PARTICIPACAO_SULAMERICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'SULAMERICA'
table_name = 'SULAMERICA_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")
table.filter_table("CODIGO_CONTRATO != 'Valor do Premio emitido desconsiderando o Aporte'")

config = {
    'CODIGO_CONTRATO' : {'type':'string'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA' : {'type':'string'},
    'ANO_REFERENCIA' : {'type':'string'},
    'MES_REFERENCIA' : {'type':'string'},
    'VALOR_PAGO' : {'type':'regex' , 'pattern': ',', 'replace': '.'},
    'BLANKS' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}   

    }

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_SULAMERICA', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'SULAMERICA'
table_name = 'SULAMERICA_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'CODIGO_CONTRATO' : {'type':'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA' : {'type':'string'},
    'CODIGO_LOCAL_EMPRESA_OPERADORA'  : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'NOME_LOCAL_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_PRODUTO_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'DESCRICAO_ELEGIBILIDADE' : {'type':'string'},
    'CODIGO_TITULAR'  : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_IDENTIFICADOR_TITULAR'  : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_BENEFICIARIO'  : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_IDENTIFICADOR_BENEFICIARIO' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'DESCRICAO_GENERO' : {'type':'string'},
    'IDADE_BENEFICIARIO' : {'type':'string'},
    'CODIGO_PLANO' : {'type':'string'},
    'DESCRICAO_PLANO' : {'type':'string'},
    'CODIGO_PRESTADOR_ORIGEM' : {'type':'string'},
    'CODIGO_PRESTADOR' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'NOME_PRESTADOR' : {'type':'string'},
    'CODIGO_CNPJ_PRESTADOR'  : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'CODIGO_CNPJ_PRESTADOR_PRINCIPAL' : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'NUMERO_CRM' : {'type':'string'},
    'NUMERO_CRM_MEDICO_SOLICITANTE' : {'type':'string'},
    'NUMERO_CRM_MEDICO_EXECUTANTE' : {'type':'string'},
    'CODIGO_SUB_TIPO_SINISTRO' : {'type':'string'},
    'CODIGO_PROCEDIMENTO_PRINCIPAL' : {'type':'string'},
    'DESCRICAO_PROCEDIMENTO_PRINCIPAL' : {'type':'string'},
    'CODIGO_PROCEDIMENTO' : {'type':'string'},
    'DESCRICAO_PROCEDIMENTO' : {'type':'string'},
    'CODIGO_AUTORIZACAO' : {'type':'string'},
    'CODIGO_TIPO_SINISTRO' : {'type':'string'},
    'DESCRICAO_TIPO_SINISTRO' : {'type':'string'},
    'CODIGO_TIPO_CATEGORIA_SINISTRO' : {'type':'string'},
    'DESCRICAO_TIPO_CATEGORIA_SINISTRO' : {'type':'string'},
    'CODIGO_POSICAO_PRESTADOR' : {'type':'string'},
    'DESCRICAO_POSICAO_PRESTADOR' : {'type':'string'},
    'VALOR_APRESENTADO' : {'type':'number_comma'},
    'VALOR_PAGO' : {'type':'number_comma'},
    'VALOR_COPARTICIPACAO' : {'type':'number_comma'},
    'VALOR_EMPRESA' : {'type':'number_comma'},
    'VALOR_NAO_REEMBOLSADO' : {'type':'number_comma'},
    'DATA_PROCEDIMENTO' : {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_REFERENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_INTERNACAO' : {"type": "date", "format": "dd/MM/yyyy"},
    'DESCRICAO_TIPO_INTERNACAO' : {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO' : {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO_PAGO' : {'type':'string'},
    'TIPO_ATENDIMENTO' : {'type':'string'},
    'NUMERO_CONTA_OPERADORA' : {'type':'string'},
    'NUMERO_LOTE_CONTA' : {'type':'string'},
    'NUMERO_GUIA_TISS_CONTA'  : {'type':'regex' , 'pattern': '"|=', 'replace': ''},
    'TIPO_GUIA_CONTA' : {'type':'string'},
    'COMPLEMENTO_TIPO_GUIA_CONTA' : {'type':'string'},
    'CODIGO_SEQUENCIA_PAGAMENTO_CONTA' : {'type':'string'},
    'NUMERO_ANEXO_CONTA' : {'type':'string'},
    'NUMERO_ITEM_CONTA' : {'type':'string'},
    'INDICA_LIMINAR' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}   

    }

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_SULAMERICA', schema='DATABRICKS_SILVER', mode='Overwrite')
#table.table.explain()

