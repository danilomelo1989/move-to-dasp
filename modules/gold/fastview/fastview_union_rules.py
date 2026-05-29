from modules.constants import DEBUG
from modules.utils.helpers import execute_itermediate_steps
from datetime import datetime
from modules.utils.connectors import SnowflakeConnector
from pyspark.sql.functions import lit, col, current_timestamp, date_format, floor, expr, rank
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, LongType, DecimalType
from pyspark.sql.window import Window


PREMIUM = StructType([
    StructField("CODIGO_LOCAL_EMPRESA_OPERADORA", StringType(), True),
    StructField("CODIGO_CONTRATO", StringType(), True),
    StructField("DATA_REFERENCIA", DateType(), True),
    StructField("CODIGO_OPERADORA", StringType(), True),
    StructField("NOME_OPERADORA", StringType(), True),
    StructField("CODIGO_EMPRESA", StringType(), True),
    StructField("NOME_EMPRESA", StringType(), True),    
    StructField("CODIGO_PLANO", StringType(), True),
    StructField("CODIGO_EMPRESA_GRUPO", StringType(), True),
    StructField("DESCRICAO_GENERO", StringType(), True),
    StructField("IDADE_BENEFICIARIO", IntegerType(), True),
    StructField("FAIXA_ETARIA", StringType(), True),
    StructField("CODIGO_TIPO_BENEFICIARIO", StringType(), True),
    StructField("CD_DIVISAO", StringType(), True),
    StructField("CODIGO_GRUPO_ECONOMICO_OPERADORA", StringType(), True),
    StructField("QUANTIDADE_BENEFICIARIO_ATIVO", LongType(), True),
    StructField("PLANO_AON_2", StringType(), True),  
    StructField("VALOR_PAGO", DecimalType(), True),
    StructField("VALOR_ACERTO", DecimalType(), True),
    StructField("VALOR_FRANQUIA", DecimalType(), True),
    StructField("VALOR_APORTE", DecimalType(), True),
    StructField("VALOR_COPARTICIPACAO", DecimalType(), True),
    StructField("ETL_SOURCE_FILE_NAME", StringType(), True),
    StructField("ETL_SOURCE_ZIP_NAME", StringType(), True),
    StructField("RUN_ID", StringType(), True),
    StructField("DATE_TIME_LOAD_STG", TimestampType(), True),
    StructField("SOURCE_FILE_NAME_LINE_NUMBER", StringType(), True),
    StructField("DATE_TIME_STG_TO_SILVER", TimestampType(), True),
    StructField("DATE_TIME_SILVER_TO_GOLD", TimestampType(), True)
])

REGISTRATION = StructType([
    StructField("CODIGO_OPERADORA", StringType(), True),
    StructField("NOME_OPERADORA", StringType(), True),
    StructField("CODIGO_CONTRATO", StringType(), True),
    StructField("CODIGO_GRUPO_ECONOMICO_OPERADORA", StringType(), True),
    StructField("CODIGO_LOCAL_EMPRESA_OPERADORA", StringType(), True),
    StructField("NOME_LOCAL_EMPRESA_OPERADORA", StringType(), True),
    StructField("CODIGO_BENEFICIARIO", StringType(), True),
    StructField("CODIGO_FAMILIA", StringType(), True),
    StructField("NOME_BENEFICIARIO", StringType(), True),
    StructField("CODIGO_CPF_TITULAR", StringType(), True),
    StructField("CODIGO_CPF_DEPENDENTE", StringType(), True),
    StructField("DATA_NASCIMENTO", DateType(), True),
    StructField("DESCRICAO_GENERO", StringType(), True),
    StructField("CODIGO_PARENTESCO", StringType(), True),
    StructField("NOME_MAE", StringType(), True),
    StructField("CODIGO_PLANO", StringType(), True),
    StructField("DATA_INCLUSAO_PLANO", DateType(), True),
    StructField("DATA_EXCLUSAO_PLANO", DateType(), True),
    StructField("DESCRICAO_ELEGIBILIDADE", StringType(), True),
    StructField("DATA_REFERENCIA", DateType(), True),
    StructField("ETL_SOURCE_FILE_NAME", StringType(), True),
    StructField("ETL_SOURCE_ZIP_NAME", StringType(), True),
    StructField("RUN_ID", StringType(), True),
    StructField("DATE_TIME_LOAD_STG", TimestampType(), True),
    StructField("SOURCE_FILE_NAME_LINE_NUMBER", StringType(), True),
    StructField("DATE_TIME_STG_TO_SILVER", TimestampType(), True),
    StructField("DATE_TIME_SILVER_TO_GOLD", TimestampType(), True),
    StructField("NOME_EMPRESA", StringType(), True),
    StructField("OBSERVACAO", StringType(), True),
    StructField("IDADE_BENEFICIARIO", IntegerType(), True),
    StructField("FAIXA_ETARIA", StringType(), True),
    StructField("CODIGO_EMPRESA", StringType(), True),
    StructField("STATUS", StringType(), True)

])

CLAIMS = StructType([
    StructField("CODIGO_OPERADORA", StringType(), True),
    StructField("NOME_OPERADORA", StringType(), True),
    StructField("DATA_REFERENCIA", DateType(), True),
    StructField("CODIGO_GRUPO_ECONOMICO_OPERADORA", DateType(), True),
    StructField("CODIGO_PRESTADOR", StringType(), True),
    StructField("CODIGO_CNPJ_PRESTADOR", StringType(), True),
    StructField("NOME_PRESTADOR", StringType(), True),
    StructField("CODIGO_PLANO", StringType(), True),
    StructField("NOME_EVENTO", StringType(), True),
    StructField("DESCRICAO_TIPO_CATEGORIA_SINISTRO", StringType(), True),
    StructField("DESCRICAO_POSICAO_PRESTADOR", StringType(), True),
    StructField("CODIGO_PROCEDIMENTO", StringType(), True),
    StructField("DESCRICAO_PROCEDIMENTO", StringType(), True),
    StructField("TIPO_ATENDIMENTO", StringType(), True),
    StructField("INICIO_INTERNACAO", DateType(), True),
    StructField("CHAVE_INTERNACAO_AON", StringType(), True),
    StructField("DATA_PROCEDIMENTO", DateType(), True),
    StructField("VALOR_PAGO", DecimalType(), True),
    StructField("VALOR_COPARTICIPACAO", DecimalType(), True),
    StructField("QUANTIDADE_PROCEDIMENTO", IntegerType(), True),
    StructField("CHAVE_TERAPIA_COMPLEXA_AON", StringType(), True),
    StructField("CHAVE_PRONTO_SOCORRO_AON", StringType(), True),
    StructField("CODIGO_BENEFICIARIO", StringType(), True),
    StructField("NM_EVNT2", StringType(), True),
    StructField("DS_SRVC_2", StringType(), True),
    StructField("DS_SRVC_ORIGINAL", StringType(), True),
    StructField("COD_TUSS", StringType(), True),
    StructField("COD_CBHPM", StringType(), True),
    StructField("PROC_PRINC", StringType(), True),
    StructField("CATEGORIA", StringType(), True),
    StructField("TIPO_INTERNACAO", StringType(), True),
    StructField("TIPO_ENTRADA_INTERNACAO", StringType(), True),
    StructField("REGIME_DIARIA", StringType(), True),
    StructField("CRONICO", StringType(), True),
    StructField("GRUPO", StringType(), True),
    StructField("CODIGO_CONTRATO", StringType(), True),     
    StructField("DS_SRVC_ABREVIADA", StringType(), True),
    StructField("CODIGO_LOCAL_EMPRESA_OPERADORA", StringType(), True),
    StructField("DESCRICAO_GENERO", StringType(), True),
    StructField("IDADE_BENEFICIARIO", IntegerType(), True),
    StructField("DESCRICAO_ELEGIBILIDADE", StringType(), True),
    StructField("ETL_SOURCE_FILE_NAME", StringType(), True),
    StructField("ETL_SOURCE_ZIP_NAME", StringType(), True),
    StructField("RUN_ID", StringType(), True),
    StructField("DATE_TIME_LOAD_STG", TimestampType(), True),
    StructField("SOURCE_FILE_NAME_LINE_NUMBER", StringType(), True),
    StructField("DATE_TIME_STG_TO_SILVER", TimestampType(), True),
    StructField("DATE_TIME_SILVER_TO_GOLD", TimestampType(), True),
    StructField("DESCRICAO_ESPECIALIDADE", StringType(), True),      
    StructField("FAIXA_ETARIA", StringType(), True),
    StructField("OBSERVACAO", StringType(), True),
    StructField("CODIGO_EMPRESA", StringType(), True),
    StructField("NOME_EMPRESA", StringType(), True)
])

def join_insurance_name(conn, table):
    insurance_name = conn.get_table_from_snowflake('PLANOS', 'AUXILIAR')  

    insurance_name= insurance_name.selectExpr( 
        "CD_OPRD",
        "NM_OPRD",
        "CD_EMPR as CODIGO_EMPRESA",
        "NM_EMPR",
        "DT_INATIVACAO",
        "CD_PLNO as CODIGO_PLANO",
        "CD_PLNO_AON",
        "DS_SIST",
        "DS_CATG_PLNO",
        "DS_REDE",
        "DS_TIPO_PLNO",
        "VL_REFR_AMB",
        "NM_PLNO_AON",
        "NOVA_CLASSIFICACAO",
        "OBSERVACAO as PLANO_AON_2",
        "DS_PLNO" 
    )
    
    table =  table \
        .join(
            insurance_name, 
            on=["CODIGO_PLANO","CODIGO_EMPRESA"], 
            how="left"
        )
    
    return table

def join_company_name(conn, table):
    company = conn.get_table_from_snowflake('EMPRESAS', 'GOLD')
    tb_empresas = conn.get_table_from_snowflake('TB_EMPRESAS', 'GOLD')
    company = company.selectExpr( 
        "CODIGO_OPERADORA",
        "CODIGO_EMPRESA",
        "CODIGO_APOLICE as CODIGO_CONTRATO"
    )
    tb_empresas = tb_empresas.selectExpr( 
        "CD_OPRD",
        "NM_EMPR as NOME_EMPRESA",
        "CD_EMPR"
    )
    table = table \
        .join(
            company, 
            on=["CODIGO_OPERADORA", "CODIGO_CONTRATO"], 
            how="left"
    ) 
    table = table \
        .join(
            tb_empresas, 
            on=[table.CODIGO_OPERADORA == tb_empresas.CD_OPRD, table.CODIGO_EMPRESA == tb_empresas.CD_EMPR], 
            how="left"
    ) 
    return table

def join_beneficiary_name(table, registration):
    registration = registration.selectExpr( 
        "DATA_REFERENCIA",
        "CODIGO_BENEFICIARIO",
        "NOME_BENEFICIARIO" ,
        "DATA_NASCIMENTO",
        "ETL_SOURCE_FILE_NAME"
    )

    ranked_registration = registration.withColumn("RANK", rank().over(Window.partitionBy("CODIGO_BENEFICIARIO").orderBy(col("DATA_REFERENCIA").desc(), col("ETL_SOURCE_FILE_NAME"))))
    top_ranked_registration = ranked_registration.filter(col("RANK") == 1) \
        .selectExpr(
        "CODIGO_BENEFICIARIO",
        "NOME_BENEFICIARIO" ,
        "DATA_NASCIMENTO") \
        .distinct()

    table = table \
        .join(
            top_ranked_registration, 
            on=["CODIGO_BENEFICIARIO"], 
            how="left"
    ) 
    return table

def adapt_layout(table, schema):  
    select_columns = []
    for field in schema.fields:
        if field.name in table.columns:
            select_columns.append(col(field.name))
        else:
            select_columns.append(lit(None).cast(StringType()).alias(field.name))
    return table.select(*select_columns) 

def calculate_age(table):
    columns = table.columns
    table.select(
        *columns,
        floor(expr("datediff(DATE_TRUNC('MONTH', DATA_REFERENCIA), DATA_NASCIMENTO) / 365.25")) \
            .alias("IDADE_BENEFICIARIO"),
        lit(expr("""
            CASE WHEN IDADE_BENEFICIARIO IS NULL THEN 'N/C' 
            WHEN IDADE_BENEFICIARIO <= 18 THEN '00 a 18'
            WHEN IDADE_BENEFICIARIO <= 23 then '19 a 23'
            WHEN IDADE_BENEFICIARIO <= 28 then '24 a 28'
            WHEN IDADE_BENEFICIARIO <= 33 then '29 a 33'
            WHEN IDADE_BENEFICIARIO <= 38 then '34 a 38'
            WHEN IDADE_BENEFICIARIO <= 43 then '39 a 43'
            WHEN IDADE_BENEFICIARIO <= 48 then '44 a 48'
            WHEN IDADE_BENEFICIARIO <= 53 then '49 a 53'
            WHEN IDADE_BENEFICIARIO <= 58 then '54 a 58'
            WHEN IDADE_BENEFICIARIO >= 59 then '59 ou +'
            ELSE 'N/C' END 
        """)).alias("FAIXA_ETARIA"),   
    )
    return table

def define_carrier_code(table, codigo_operadora = '', nome_operadora = ''):
    table = table.withColumn("DATE_TIME_SILVER_TO_GOLD", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
    table = table.withColumn("CODIGO_OPERADORA", lit(codigo_operadora))
    table = table.withColumn("NOME_OPERADORA", lit(nome_operadora))

    return table