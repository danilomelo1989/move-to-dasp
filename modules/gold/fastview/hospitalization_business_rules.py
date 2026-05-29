
from pyspark.sql import functions
from pyspark.sql.window import Window
from modules.constants import DEBUG

from modules.utils.helpers import execute_itermediate_steps
from modules.utils.connectors import SnowflakeConnector

from modules.utils.connectors import SnowflakeConnector
from pyspark.sql.functions import lit, col, current_timestamp, date_format
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, LongType, DecimalType

from modules.gold.fastview.hospitalization_rules_udf import group_by_days

@execute_itermediate_steps(False)
def generate_generic_hospitalization_key(
    table,
):
    columns = table.columns
    hospitalization_key = table \
        .select(
            "CODIGO_CONTRATO",
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "CODIGO_BENEFICIARIO",
            "DATA_PROCEDIMENTO",
            functions \
                .when(
                    functions.col('DESCRICAO_TIPO_INTERNACAO') == 'INTERNADO',
                    functions.concat(
                        functions.lit('INT'),
                        functions.col("CODIGO_CONTRATO"),
                        functions.col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
                        functions.col("CODIGO_BENEFICIARIO"),
                        functions.col("DATA_PROCEDIMENTO")
                    )
                ) \
                .otherwise(functions.lit(None)) \
                .alias("CHAVE_INTERNACAO_AON")
        ) \
        .distinct()

    columns = hospitalization_key.columns
    window_spec = Window \
        .partitionBy(
            "CODIGO_CONTRATO", 
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "CODIGO_BENEFICIARIO"
        ) \
        .orderBy(
            "DATA_PROCEDIMENTO"
        )
    lead_date = hospitalization_key \
        .select(
            *columns,
            functions.lead("DATA_PROCEDIMENTO").over(window_spec).alias("NEXT_DAY")
        )

    hospitalization_keys = lead_date \
        .repartition("CODIGO_CONTRATO", "CODIGO_GRUPO_ECONOMICO_OPERADORA") \
        .orderBy(
            "CODIGO_CONTRATO", 
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "CODIGO_BENEFICIARIO",
            "DATA_PROCEDIMENTO"
        ) \
        .rdd \
        .mapPartitions(group_by_days) \
        .toDF([
            "UDF_CODIGO_CONTRATO", 
            "UDF_CODIGO_GRUPO_ECONOMICO_OPERADORA",
            "UDF_CODIGO_BENEFICIARIO", 
            "UDF_INTERNACAO_AON",
            "INICIO_INTERNACAO",
            "FIM_INTERNACAO"
        ])

    window_total_hospitalization  = Window.partitionBy("UDF_INTERNACAO_AON")
    columns = table.columns
    table = table \
        .join(
            hospitalization_keys,
            (table['CODIGO_CONTRATO'] == hospitalization_keys['UDF_CODIGO_CONTRATO'])
            & (table['CODIGO_GRUPO_ECONOMICO_OPERADORA'] == hospitalization_keys['UDF_CODIGO_GRUPO_ECONOMICO_OPERADORA'])
            & (table['CODIGO_BENEFICIARIO'] == hospitalization_keys['UDF_CODIGO_BENEFICIARIO'])
            & (
                table['DATA_PROCEDIMENTO'].between(
                    hospitalization_keys['INICIO_INTERNACAO'], 
                    hospitalization_keys['FIM_INTERNACAO']
                )
            ),
            'LEFT'
        ) \
        .select(
            *columns,
            functions.sum("VALOR_PAGO").over(window_total_hospitalization).alias("CUSTO_TOTAL_INTERNACAO"),
            functions \
                .when(functions.col("CUSTO_TOTAL_INTERNACAO") > "500", functions.col("UDF_INTERNACAO_AON")) \
                .otherwise(functions.lit(None)) \
                .alias('CHAVE_INTERNACAO_AON'),
            functions \
                .when(functions.col("CHAVE_INTERNACAO_AON").isNotNull(), functions.lit("INTERNACAO")) \
                .otherwise(functions.lit(None)) \
                .alias("NOME_EVENTO"),
            "INICIO_INTERNACAO"
        )

    return table


@execute_itermediate_steps(False)
def classify_events(
    table, 
    classifications,
    code_name="CODIGO_PROCEDIMENTO", 
    description_name="DESCRICAO_PROCEDIMENTO"
):
  
    columns = [
        "CD_SRVC",
        "DS_SRVC_ORIGINAL",        
        "CATEGORIA",
        "COD_TUSS",
        "PROC_PRINC",
        "COD_CBHPM",
        "DS_SRVC_2",
        "TIPO_INTERNACAO",
        "CRONICO",
        "REGIME_DIARIA",
        "NM_EVNT2",
        "GRUPO"
    ]

    table = table.join(
        classifications.select(columns),
        (table[code_name] == classifications['CD_SRVC'])
        & (table[description_name] == classifications['DS_SRVC_ORIGINAL']),
        'LEFT'
    )

    columns = list(set(table.columns) - {'NOME_EVENTO', 'CHAVE_INTERNACAO_AON'})
    table = table \
        .select(
            *columns,
            functions
                .when(table.NM_EVNT2 == 'CONSULTAELETIVA', functions.lit(None)) \
                .when(table.NM_EVNT2 == 'CONSULTAPS', functions.lit(None)) \
                .when(table.NOME_EVENTO.isNotNull(), table.NOME_EVENTO) \
                .otherwise(functions.lit(None))\
                .alias('NOME_EVENTO'),
            functions
                .when(
                    functions.col('NOME_EVENTO').isNotNull(), 
                    functions.col('CHAVE_INTERNACAO_AON')
                ) \
                .otherwise(functions.lit(None))
                .alias('CHAVE_INTERNACAO_AON'),
                
        )\

    return table


@execute_itermediate_steps(False)
def define_main_procedures(table):
    #TODO Pode acontecer de a internacao n ter nenhum proc_princ, tratar esses casos 
    main_procedure = table \
        .filter('PROC_PRINC = 1 AND NOME_EVENTO = "INTERNACAO"') \
        .withColumn(
            "rank", 
            functions.row_number() \
                .over(Window.partitionBy("CHAVE_INTERNACAO_AON") \
                .orderBy(functions.desc("VALOR_PAGO")))
        ).filter("rank = 1") \
        .select(
            "CHAVE_INTERNACAO_AON", 
            functions.col("COD_TUSS").alias('PROCEDIMENTO_PRINCIPAL'), 
            functions.when(
                    functions.col("TIPO_INTERNACAO").isNotNull(),
                    functions.col("TIPO_INTERNACAO")
                ) \
                .otherwise("Clínica") \
                .alias('TIPO_INTERNACAO') 
        )

    columns = list(set(table.columns) - {'TIPO_INTERNACAO'})
    table = table.select(columns).join(
        main_procedure,
        'CHAVE_INTERNACAO_AON',
        'LEFT'
    )    

    return table

@execute_itermediate_steps(False)
def define_entrance_type(table, beneficiary_id="CODIGO_BENEFICIARIO"):
    hospitalization_entrance = table \
        .filter("NM_EVNT2 = 'CONSULTAPS'") \
        .select(
            functions.col("DATA_PROCEDIMENTO").alias("INICIO_INTERNACAO"),  #TODO TIRAR ESSE ALIAS COISA FEIA
            functions.col(beneficiary_id),
            functions.lit("Urgência").alias("TIPO_ENTRADA_INTERNACAO")
        ) \
        .distinct()
    hospitalization_by_start_date = table \
        .filter(functions.col("CHAVE_INTERNACAO_AON").isNotNull()) \
        .select(beneficiary_id, "INICIO_INTERNACAO", "CHAVE_INTERNACAO_AON") \
        .join(
            hospitalization_entrance,
            [beneficiary_id, "INICIO_INTERNACAO"]
        ) \
        .select("CHAVE_INTERNACAO_AON", "TIPO_ENTRADA_INTERNACAO") \
        .distinct()

    columns = table.columns
    table = table \
        .join(
            hospitalization_by_start_date,
            "CHAVE_INTERNACAO_AON",
            "LEFT"
        ) \
        .select(
            *columns, 
            functions.when(
                    functions.col('TIPO_ENTRADA_INTERNACAO').isNotNull(),
                    functions.col('TIPO_ENTRADA_INTERNACAO')
                ) \
                .when(
                    functions.col('CHAVE_INTERNACAO_AON').isNotNull(),
                    functions.lit('Eletiva')
                ) \
                .otherwise(functions.lit(None)).alias('TIPO_ENTRADA_INTERNACAO')
        )  

    return table  

@execute_itermediate_steps(False)

def generate_complex_treatment_key(table, beneficiary_id="CODIGO_BENEFICIARIO"):
    complex_treatment = table \
        .filter("NM_EVNT2 = 'TERAPIACOMPLEXA' AND NOME_EVENTO IS NULL")\
        .select(

            "CODIGO_CONTRATO",
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            beneficiary_id,
            "DATA_PROCEDIMENTO",
            functions.concat(
                    functions.lit('TC'),
                    functions.col("CODIGO_CONTRATO"),
                    functions.col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
                    functions.col(beneficiary_id),
                    functions.col("DATA_PROCEDIMENTO")
                ).alias('CHAVE_TERAPIA_COMPLEXA_AON')
        ) \
        .distinct()

    columns = list(set(table.columns) - {'NOME_EVENTO', 'CHAVE_INTERNACAO_AON'})
    table = table \
        .join(
            complex_treatment,

            [
                "CODIGO_CONTRATO",
                "CODIGO_GRUPO_ECONOMICO_OPERADORA",
                beneficiary_id,
                "DATA_PROCEDIMENTO"
            ],
            "LEFT"
        ) \
        .select(
            *columns,
            "CHAVE_TERAPIA_COMPLEXA_AON",
            functions.when(
                    functions.col("CHAVE_TERAPIA_COMPLEXA_AON").isNotNull(),
                    functions.lit('TERAPIACOMPLEXA')
                )
                .when(
                    functions.col("NOME_EVENTO").isNotNull(), 
                    functions.col("NOME_EVENTO")
                ) \
                .otherwise(functions.lit(None)) \
                .alias("NOME_EVENTO"),
            functions.when(
                    functions.col("CHAVE_TERAPIA_COMPLEXA_AON").isNotNull(),
                    functions.lit(None)
                ) \
                .otherwise(
                    functions.col("CHAVE_INTERNACAO_AON")
                ).alias('CHAVE_INTERNACAO_AON')
        )

    return table

@execute_itermediate_steps(True)
def generate_emergency_care_key(table, beneficiary_id="CODIGO_BENEFICIARIO"):
    columns = table.columns
    emergency = table \
        .filter("NM_EVNT2 = 'CONSULTAPS' AND NOME_EVENTO IS NULL")\
        .select(

            "CODIGO_CONTRATO",
            "CODIGO_GRUPO_ECONOMICO_OPERADORA",
            beneficiary_id,
            "DATA_PROCEDIMENTO",
            functions.concat(
                    functions.lit('PS'),
                    functions.col("CODIGO_CONTRATO"),
                    functions.col("CODIGO_GRUPO_ECONOMICO_OPERADORA"),
                    functions.col(beneficiary_id),
                    functions.col("DATA_PROCEDIMENTO")
                ).alias('CHAVE_PRONTO_SOCORRO_AON')
        ) \
        .distinct()

    columns = list(set(table.columns) - {'NOME_EVENTO', 'CHAVE_INTERNACAO_AON'})
    table = table \
        .join(
            emergency,

            [
                "CODIGO_CONTRATO",
                "CODIGO_GRUPO_ECONOMICO_OPERADORA",
                beneficiary_id,
                "DATA_PROCEDIMENTO"
            ],
            "LEFT"
        ) \
        .select(*columns,
            "CHAVE_PRONTO_SOCORRO_AON",
            functions.when(
                    functions.col("CHAVE_PRONTO_SOCORRO_AON").isNotNull(),
                    functions.lit('CONSULTAPS')
                ) \
                .when(
                    functions.col("NOME_EVENTO").isNotNull(), 
                    functions.col("NOME_EVENTO")
                )
                .alias("NOME_EVENTO"),
            functions.when(
                    functions.col("CHAVE_PRONTO_SOCORRO_AON").isNotNull(),
                    functions.lit(None)
                ) \
                .otherwise(
                    functions.col("CHAVE_INTERNACAO_AON")
                ).alias('CHAVE_INTERNACAO_AON')
        )

    columns = list(set(table.columns) - {"NM_EVNT2"})
    table = table.select(*columns,
            functions.when(
                functions.col("NOME_EVENTO").isNotNull(),
                functions.col("NOME_EVENTO")
            ) \
            .when(
                functions.col("NOME_EVENTO").isNull()
                & functions.col("NM_EVNT2").isNotNull(),
                functions.col("NM_EVNT2")
            ) \
            .otherwise(functions.lit("AMBULATORIO")) \
            .alias("NM_EVNT2")
        )

    return table



unified_schema = StructType([
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
    StructField("DATE_TIME_SILVER_TO_GOLD", TimestampType(), True)
])

def adapt_layout(df, schema):
    select_columns = []
    for field in schema.fields:
        if field.name in df.columns:
            select_columns.append(col(field.name))
        else:
            select_columns.append(lit(None).cast(StringType()).alias(field.name))
    return df.select(*select_columns)

def append_to_snowflake_table(conn, df, table_name, schema_name):
    adapted_df = adapt_layout(df, unified_schema)    
    conn.save_snowflake_table(adapted_df, table_name, schema_name, 'Append')

def process_and_append_tables(conn, df, schema, codigo_operadora = '', nome_operadora = ''):        
        df = df.withColumn("DATE_TIME_SILVER_TO_GOLD", current_timestamp())
        df = df.withColumn("CODIGO_OPERADORA", lit(codigo_operadora))
        df = df.withColumn("NOME_OPERADORA", lit(nome_operadora))
        append_to_snowflake_table(conn, df, 'TESTE_SINISTRO_UNIFICADO', 'GOLD')
