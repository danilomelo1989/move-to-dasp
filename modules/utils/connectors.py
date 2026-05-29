import json
import boto3

from modules.constants import *
from modules.utils.readers import read_secret_aws_secrets
from pyspark.sql import SparkSession

class SnowflakeConnector():
    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.conn = self._get_conn(self.spark)
    
    def get_snowflake_table(self, table: str,  schema: str) -> object:
        df_table = self.spark.read  \
            .format("snowflake")    \
            .options(**self.conn)   \
            .option("schema", schema)  \
            .option("dbtable", table)   \
            .load()

        return df_table

    def save_snowflake_table(self, table: object, table_name: str, schema: str, mode:str) -> object:
        table.write    \
            .format("snowflake")    \
            .options(**self.conn)   \
            .option("schema", schema)  \
            .option("dbtable", table_name)   \
            .mode(mode)   \
            .save()

        return

    def get_table_from_snowflake(self, table: str,  schema: str) -> object:
        table = self.spark.read  \
            .format("snowflake")    \
            .options(**self.conn)   \
            .option("schema", schema)  \
            .option("dbtable", table)   \
            .load()

        return table

    def upload_table_to_snowflake(self, table: object, table_name: str, schema: str, mode: str='append') -> object:
        table.write    \
            .format("snowflake")    \
            .options(**self.conn)   \
            .option("schema", schema)  \
            .option("dbtable", table_name)   \
            .mode(mode) \
            .save()

        return

    def _get_conn(self, spark) -> dict:
        secret = read_secret_aws_secrets(
            f'{SECRETS_PATH}/{SNOWFLAKE_SECRET_NAME}',
            SECRETS_REGION
        )

        db_name = SNOWFLAKE_DATABASE
        warehouse = secret['warehouse']
        role = secret['role']

        snowflake_connection_conf = {
                "sfUrl": f"{secret['account']}.snowflakecomputing.com",
                "sfUser": secret['username'],
                "sfPassword": secret['password'],
                "sfDatabase": f'"{db_name}"',
                "sfWarehouse": f'"{warehouse}"',
                "sfRole": f'"{role}"',
                "sfAuthenticator": "https://aon.okta.com",
                "useProxy": "true",
                "proxyHost": "serverproxy.aon.net",
                "proxyPort": "8888",
                "noProxy": "*.privatelink.snowflakecomputing.com"
        }

        return snowflake_connection_conf
