# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # BRADESCO

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_PREMIO_1'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'TIPO_ARQUIVO' : {'type':'string'},
    'NOME_GRUPO_ECONOMICO_OPERADORA' : {'type':'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA' : {'type':'string'},
    'CODIGO_CENTRO_CUSTO' : {'type':'string'},
    'CODIGO_CONTRATO' : {'type':'string'},
    'CODIGO_RAMO_EMPRESA' : {'type':'string'},
    'DESCRICAO_RAMO_EMPRESA' : {'type':'string'},
    'DATA_REFERENCIA'  : {"type": "date", "format": "yyyymm"},
    'DATA_PROCESSAMENTO' : {"type": "date", "format": "yyyymmdd"},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'RUN_ID': {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_1_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_PREMIO_2'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'TIPO_REGISTRO' : {'type':'string'},
    'SEQUENCIA_SUBFATURA' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'NOME_EMPRESA_OPERADORA' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_2_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_PREMIO_3'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'CODIGO_DEPENDENCIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'INDICATIVO_SUBFATURA_ATUAL_ANTERIOR' : {'type':'string'},
    'DATA_NASCIMENTO' : {"type": "date", "format": "ddmmyyyy"},
    'CODIGO_GENERO' : {'type':'string'},
    'CODIGO_ESTADO_CIVIL' : {'type':'string'},
    'CODIGO_PARENTESCO' : {'type':'string'},
    'CODIGO_PLANO' : {'type':'string'},
    'DATA_INCLUSAO_PLANO' :{"type": "date", "format": "ddmmyyyy"},
    'CODIGO_TIPO_PAGAMENTO' : {'type':'string'},
    'DATA_PAGAMENTO' : {'type':'string'},
    'VALOR_PAGO' : {'type':'money'},
    'VALOR_COPARTICIPACAO' : {'type':'money'},
    'CODIGO_PAGAMENTO' : {'type':'string'},
    'DESCRICAO_CARGO' : {'type':'string'},
    'CODIGO_MATRICULA_ESPECIAL' : {'type':'string'},
    'CAMPO_NAO_UTILIZADO' : {'type':'string'},
    'CODIGO_CPF' : {'type':'string'},
    'CODIGO_CNS' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_3_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_PREMIO_4'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_TIPO_PAGAMENTO' : {'type':'string'},
    'TOTAL_TITULARES' : {'type':'string'},
    'TOTAL_DEPENDENTES' : {'type':'string'},
    'TOTAL_SEGURADOS' : {'type':'string'},
    'TOTAL_LANCAMENTOS_QUANTIDADE' : {'type':'string'},
    'TOTAL_LANCAMENTOS_VALOR' : {'type':'money'},
    'PARTE_SEGURADO_VALOR' : {'type':'money'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('PREMIO_4_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_SINISTRO_H'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_CONTRATO' : {'type':'string'},
    'CAMPO_FILLER' : {'type':'string'},
    'DATA_REFERENCIA' : {"type": "date", "format": "yyyymm"},
    'DATA_PROCESSAMENTO' :{"type": "date", "format": "yyyymmdd"},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_H_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_SINISTRO_M'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'NOME_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_TIPO_SUBFATURA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'CODIGO_MATRICULA' : {'type':'string'},
    'NOME_TITULAR' : {'type':'string'},
    'CODIGO_DEPENDENCIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'NOME_PRESTADOR' : {'type':'string'},
    'CODIGO_TIPO_SINISTRO' : {'type':'string'},
    'CODIGO_SINISTRO' : {'type':'string'},
    'CODIGO_PROCEDIMENTO' : {'type':'string'},
    'QUANTIDADE_PROCEDIMENTO' : {'type':'string'},
    'VALOR_PAGO' : {'type':'money'},
    'CODIGO_CONTRATO_SERVICO' : {'type':'string'},
    'CODIGO_PRESTADOR' : {'type':'string'},
    'FLAG_SADT_INTERNADO' : {'type':'string'},
    'CAMPO_FILLER' : {'type':'string'},
    'CODIGO_GENERO' : {'type':'string'},
    'CODIGO_PARENTESCO' : {'type':'string'},
    'CODIGO_ESPECIALIDADE' : {'type':'string'},
    'VALOR_SINISTRO' : {'type':'money'},
    'CODIGO_MATRICULA_ESPECIAL' : {'type':'string'},
    'CODIGO_TIPO_BENEFICIARIO' : {'type':'string'},
    'CODIGO_AUTORIZACAO_PROCEDIMENTO' : {'type':'string'},
    'CODIGO_CNPJ_PRESTADOR' : {'type':'string'},
    'CODIGO_TIPO_PRESTADOR' : {'type':'string'},
    'VALOR_INSS_ISS' : {'type':'money'},
    'VALOR_INSS_ISS_FAJ_TR' : {'type':'money'},
    'DESCRICAO_CARGO' : {'type':'string'},
    'DATA_ADMISSAO'  :{"type": "date", "format": "yyyymmdd"},
    'CODIGO_PLANO' : {'type':'string'},
    'CODIGO_CID' : {'type':'string'},
    'DATA_PAGAMENTO' :{"type": "date", "format": "yyyymmdd"},
    'DATA_PROCEDIMENTO' :{"type": "date", "format": "yyyymmdd"},
    'DATA_NASCIMENTO' :{"type": "date", "format": "yyyymmdd"},
    'FLAG_TROCA_ACOMODACAO' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_M_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_SINISTRO_T'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'TOTAL_REGISTROS' : {'type':'string'},
    'TOTAL_VALOR_SINISTRO' : {'type':'money'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('SINISTRO_T_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_GERENCIAL'
file_type = 'GERENCIAL'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'DATA_VALOR' : {'type':'string'},
    'VALOR_PREMIO_GERENCIAL' : {'type':'money'},
    'VALOR_SINISTRO_GERENCIAL' : {'type':'money'},
    'PERCENTUAL_SINISTRO_GERENCIAL' : {'type':'string'},
    'QUANTIDADE_VIDAS' : {'type':'string'},
    'VALOR_PREMIO_PER_CAPITA' : {'type':'money'},
    'VALOR_SINISTRO_PER_CAPITA' : {'type':'money'},
    'TAXA_ADM' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},    
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('GERENCIAL_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_1'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'TIPO_REGISTRO' : {'type':'string'},
    'TIPO_ARQUIVO' : {'type':'string'},
    'CODIGO_CONTRATO' : {'type':'string'},
    'DATA_REFERENCIA' :{"type": "date", "format": "yyyymmdd"},
    'DATA_PROCESSAMENTO':{"type": "date", "format": "yyyymmdd"},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_1_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_2'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {

    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'CODIGO_MATRICULA' : {'type':'string'},
    'DESCRICAO_GENERO' : {'type':'string'},
    'DESCRICAO_ESTADO_CIVIL' : {'type':'string'},
    'CPF' : {'type':'string'},
    'NOME_CARGO' : {'type':'string'},
    'CODIGO_PLANO' : {'type':'string'},
    'CODIGO_MATRICULA_ESPECIAL' : {'type':'string'},
    'DATA_NASCIMENTO' :{"type": "date", "format": "yyyymmdd"},
    'DATA_ADMISSAO' :{"type": "date", "format": "yyyymmdd"},
    'DATA_INCLUSAO_PLANO':{"type": "date", "format": "yyyymmdd"},
    'AREA_RESERVADA' : {'type':'string'},
    'DATA_REATIVACAO_PLANO' :{"type": "date", "format": "yyyymmdd"},
    'REGIAO' : {'type':'string'},
    'DATA_EXCLUSAO_PLANO' :{"type": "date", "format": "yyyymmdd"},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}



    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_2_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_3'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'CODIGO_DEPENDENCIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'DESCRICAO_GENERO' : {'type':'string'},
    'CODIGO_ESTADO_CIVIL' : {'type':'string'},
    'DESCRICAO_PARENTESCO' : {'type':'string'},
    'CODIGO_MATRICULA_ESPECIAL_TITULAR' : {'type':'string'},
    'DATA_NASCIMENTO'  :{"type": "date", "format": "yyyymmdd"},
    'DATA_INCLUSAO_PLANO' :{"type": "date", "format": "yyyymmdd"},
    'CODIGO_MATRICULA_ESPECIAL_DEPENDENTE' : {'type':'string'},
    'DATA_REATIVACAO_PLANO' :{"type": "date", "format": "yyyymmdd"},
    'DATA_EXCLUSAO_PLANO'  :{"type": "date", "format": "yyyymmdd"},
    'CPF' : {'type':'string'},
    'CODIGO_CARTEIRINHA' : {'type':'string'},
    'NOME_MAE' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}

    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_3_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_4'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'TOTAL_TITULAR' : {'type':'string'},
    'TOTAL_DADOS_BANCARIOS' : {'type':'string'},
    'TOTAL_CORRENTISTA' : {'type':'string'},
    'TOTAL_DEPENDENTE' : {'type':'string'},
    'TOTAL_REGISTRO' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_4_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_5'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'CODIGO_MATRICULA' : {'type':'string'},
    'CODIGO_CARTEIRINHA' : {'type':'string'},
    'NOME_MAE' : {'type':'string'},
    'CODIGO_MATRICULA_ESPECIAL' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
  
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_5_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_6'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'CODIGO_MATRICULA' : {'type':'string'},
    'TIPO_DOCUMENTO' : {'type':'string'},
    'NUMERO_IDENTIDADE' : {'type':'string'},
    'CODIGO_INSTITUICAO_EMISSOR' : {'type':'string'},
    'CODIGO_PAIS_EMISSOR' : {'type':'string'},
    'DATA_EMISSAO_IDENTIDADE'  :{"type": "date", "format": "yyyymmdd"},
    'NUMERO_PIS' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_6_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

carrier = 'BRADESCO'
table_name = 'BRADESCO_CADASTRO_9'
file_type = 'CADASTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER", datetim)

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

config = {
    'TIPO_REGISTRO' : {'type':'string'},
    'CODIGO_EMPRESA_OPERADORA' : {'type':'string'},
    'CODIGO_FAMILIA' : {'type':'string'},
    'NOME_BENEFICIARIO' : {'type':'string'},
    'CODIGO_MATRICULA' : {'type':'string'},
    'CODIGO_BANCO' : {'type':'string'},
    'CODIGO_AGENCIA' : {'type':'string'},
    'CODIGO_PRIMEIRO_DIGITO_AGENCIA' : {'type':'string'},
    'CODIGO_SEGUNDO_DIGITO_AGENCIA' : {'type':'string'},
    'CODIGO_CONTA_CORRENTE' : {'type':'string'},
    'CODIGO_PRIMEIRO_DIGITO_CONTA' : {'type':'string'},
    'CODIGO_SEGUNDO_DIGITO_CONTA' : {'type':'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},    
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}  
    }

table.column_cast(config)
table.upload_table_to_snowflake('CADASTRO_9_BRADESCO', schema='DATABRICKS_SILVER', mode='Overwrite')
