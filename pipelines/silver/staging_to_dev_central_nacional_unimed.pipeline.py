# Databricks notebook source
# MAGIC %pip install tabula-py

# COMMAND ----------

from modules.staging.data_type_transformations import Table
from datetime import datetime
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC # CNU

# COMMAND ----------

# MAGIC %md
# MAGIC ## PREMIO

# COMMAND ----------

carrier = 'CENTRAL_NACIONAL_UNIMED'
table_name = 'CENTRAL_NACIONAL_UNIMED_PREMIO'
file_type = 'PREMIO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

central_nacional_unimed_premio_config = {
    'CODIGO_OPERADORA_ARQUIVO': {'type': 'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type': 'string'},
    'NOME_LOTACAO_EMPRESA_OPERADORA': {'type': 'string'},
    'DATA_REFERENCIA': {"type": "date", "format": "yyyyMM"},
    'CODIGO_TIPO_OPERACAO': {'type': 'string'},
    'DATA_VIGENCIA_TIPO_OPERACAO': {"type": "date", "format": "yyyyMM"},
    'CODIGO_MATRICULA': {'type': 'string'},
    'CODIGO_PARENTESCO': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'CODIGO_CPF': {'type': 'string'},
    'CODIGO_BENEFICIARIO': {'type': 'string'},
    'DESCRICAO_GENERO': {'type': 'string'},
    'DESCRICAO_ESTADO_CIVIL': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'CODIGO_CPF_TITULAR': {'type': 'string'},
    'DATA_INICIO_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'SITUACAO_BENEFICIARIO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'IDADE_LIMITE_BENEFICIARIO': {'type': 'integer'},
    'VALOR_PAGO': {'type': 'money'},
    'SINAL_VALOR': {'type': 'string'},
    'VALOR_INSCRICAO': {'type': 'money'},
    'NUMERO_FATURA': {'type': 'string'},
    'CODIGO_CENTRO_CUSTO': {'type': 'string'},
    'NOME_EMPRESA_OPERADORA': {'type': 'string'},
    'CODIGO_LOTACAO': {'type': 'string'},
    'CODIGO_CNPJ_CONTRATO': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}
}

table.column_cast(central_nacional_unimed_premio_config)
table.upload_table_to_snowflake('PREMIO_CENTRAL_NACIONAL_UNIMED', schema='DATABRICKS_SILVER', mode='Overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SINISTRO

# COMMAND ----------

carrier = 'CENTRAL_NACIONAL_UNIMED'
table_name = 'CENTRAL_NACIONAL_UNIMED_SINISTRO'
file_type = 'SINISTRO'

table = Table(table_name)

datetim =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
table.add_column("DATE_TIME_STG_TO_SILVER" ,datetim )

table.add_dense_rank("ETL_SOURCE_ZIP_NAME" , "DATE_TIME")
table.filter_table("DENSE_RANK = 1")

central_nacional_unimed_sinistro_config = {
    'CODIGO_CONTRATO': {'type': 'string'},
    'NOME_CONTRATO': {'type': 'string'},
    'CODIGO_EMPRESA_OPERADORA': {'type': 'string'},
    'NOME_EMPRESA_OPERADORA': {'type': 'string'},
    'CODIGO_SUBCONTRATO': {'type': 'string'},
    'CODIGO_LOTACAO': {'type': 'string'},
    'NOME_LOTACAO': {'type': 'string'},
    'CODIGO_SETOR_BENEFICIARIO_EMPRESA': {'type': 'string'},
    'DESCRICAO_SETOR_BENEFICIARIO_EMPRESA': {'type': 'string'},
    'CODIGO_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'NOME_GRUPO_ECONOMICO_OPERADORA': {'type': 'string'},
    'DATA_REFERENCIA': {"type": "date", "format": "MM/yyyy"},
    'CODIGO_FAMILIA': {'type': 'string'},
    'CODIGO_MATRICULA': {'type': 'string'},
    'CODIGO_SEQUENCIAL_MATRICULA': {'type': 'string'},
    'CODIGO_CARTEIRINHA_TITULAR': {'type': 'string'},
    'CODIGO_CPF_TITULAR': {'type': 'string'},
    'NOME_TITULAR': {'type': 'string'},
    'CODIGO_CARTEIRINHA': {'type': 'string'},
    'NOME_BENEFICIARIO': {'type': 'string'},
    'CODIGO_PLANO': {'type': 'string'},
    'DESCRICAO_PLANO': {'type': 'string'},
    'DESCRICAO_GENERO': {'type': 'string'},
    'DATA_NASCIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'DESCRICAO_PARENTESCO': {'type': 'string'},
    'INDICA_APOSENTADO_DEMITIDO': {'type': 'string'},
    'UF_BENEFICIARIO': {'type': 'string'},
    'DATA_PROCEDIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'CODIGO_PROCEDIMENTO': {'type': 'string'},
    'DESCRICAO_PROCEDIMENTO': {'type': 'string'},
    'DATA_INTERNACAO': {"type": "date", "format": "dd/MM/yyyy"},
    'DATA_ALTA_PROCEDIMENTO': {"type": "date", "format": "dd/MM/yyyy"},
    'DESCRICAO_TIPO_SINISTRO': {'type': 'string'},
    'CODIGO_CID': {'type': 'string'},
    'TIPO_ACOMODACAO_PLANO': {'type': 'string'},
    'CODIGO_AUTORIZACAO': {'type': 'string'},
    'INDICA_INTERNADO': {'type': 'string'},
    'QUANTIDADE_PROCEDIMENTO': {'type': 'integer'},
    'DATA_COBRANCA_COMPETENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'NUMERO_NR': {'type': 'string'},
    'NUMERO_CONTA_OPERADORA': {'type': 'string'},
    'NUMERO_ITEM_CONTA': {'type': 'string'},
    'VALOR_COPARTICIPACAO': {'type': 'money'},
    'VALOR_COPARTICPACAO_FATURADO': {'type': 'money'},
    'VALOR_APRESENTADO': {'type': 'money'},
    'NUMERO_FATURA_CUSTO_OPERACIONAL': {'type': 'string'},
    'NUMERO_FATURA_COPARTICIPACAO': {'type': 'string'},
    'DATA_COBRANCA': {"type": "date", "format": "dd/MM/yyyy"},
    'NUMERO_UNIMED': {'type': 'string'},
    'DESCRICAO_TIPO_INCLUSAO': {'type': 'string'},
    'DATA_PROCESSAMENTO_REVISAO': {"type": "date", "format": "dd/MM/yyyy"},
    'USO_OPERADORA': {'type': 'string'},
    'ANO_MES_UTILIZACAO': {'type': 'string'},
    'CODIGO_SUB_TIPO_SINISTRO': {'type': 'string'},
    'DESCRICAO_SUB_TIPO_SINISTRO': {'type': 'string'},
    'INDICA_IR': {'type': 'string'},
    'INDICA_INSS': {'type': 'string'},
    'INDICA_ISS': {'type': 'string'},
    'INDICA_REEMBOLSO': {'type': 'string'},
    'VALOR_APRESENTADO_REEMBOLSO': {'type': 'money'},
    'VALOR_PROCEDIMENTO_REEMBOLSO': {'type': 'money'},
    'VALOR_PAGO_REEMBOLSO': {'type': 'money'},
    'DESCRICAO_PRESTADOR_COOPERADO_NAO_COOPERADO': {'type': 'string'},
    'CODIGO_PRESTADOR': {'type': 'string'},
    'NOME_PRESTADOR': {'type': 'string'},
    'CODIGO_TIPO_PRESTADOR': {'type': 'string'},
    'TIPO_ATENDIMENTO': {'type': 'string'},
    'CODIGO_CNPJ_PRESTADOR': {'type': 'string'},
    'DESCRICAO_ESPECIALIDADE': {'type': 'string'},
    'CODIGO_PRESTADOR_ORIGEM': {'type': 'string'},
    'NOME_PRESTADOR_ORIGEM': {'type': 'string'},
    'VALOR_BASE_CALCULO_COPARTICIPACAO': {'type': 'money'},
    'DATA_FIM_VIGENCIA': {"type": "date", "format": "dd/MM/yyyy"},
    'HORARIO_ATENDIMENTO': {'type': 'string'},
    'DESCRICAO_TIPO_CUSTO_CONTRATO': {'type': 'string'},
    'INDICA_PACOTE': {'type': 'string'},
    'DESCRICAO_MATERIAL_ESPECIAL_OPME': {'type': 'string'},
    'VALOR_COPARTICIPACAO_INATIVO': {'type': 'money'},
    'NUMERO_TS_ITEM_OPERADORA': {'type': 'string'},
    'INDICA_ISENCAO_COBRANCA_COPARTICIPACAO': {'type': 'string'},
    'PERCENTUAL_CALCULO_COPARTICIPACAO': {'type': 'string'},
    'CONTAGEM_COPARTICIPACAO_REGRA_OPERADORA': {'type': 'string'},
    'INDICA_HORARIO_EMERGENCIA': {'type': 'string'},
    'DESCRICAO_POSICAO_PRESTADOR': {'type': 'string'},
    'CODIGO_TABELA_ANS': {'type': 'string'},
    'NOME_PRESTADOR_EXECUTANTE': {'type': 'string'},
    'UNIDADE_MEDIDA': {'type': 'string'},
    'DESCRICAO_UNIDADE_MEDIDA': {'type': 'string'},
    'CODIGO_PRESTADOR_CLASSIFICACAO_BRASILEIRA_OCUPACAO': {'type': 'string'},
    'DESCRICAO_PRESTADOR_CLASSIFICACAO_BRASILEIRA_OCUPACAO': {'type': 'string'},
    'INDICA_ACAO_JUDICIAL': {'type': 'string'},
    'INDICA_INTERNACAO': {'type': 'string'},
    'CAPITATION': {'type': 'string'},
    'ETL_SOURCE_FILE_NAME' : {'type':'string'},
    'ETL_SOURCE_ZIP_NAME' : {'type':'string'},
    'RUN_ID' : {'type':'string'},
    'DATE_TIME': {'type': 'timestamp', 'rename': 'DATE_TIME_LOAD_STG'},
    'DATE_TIME_STG_TO_SILVER': {'type': 'timestamp'},
    'SOURCE_FILE_NAME_LINE_NUMBER' : {'type':'integer'}

}

table.column_cast(central_nacional_unimed_sinistro_config)
table.upload_table_to_snowflake('SINISTRO_CENTRAL_NACIONAL_UNIMED', schema='DATABRICKS_SILVER', mode='Overwrite')
