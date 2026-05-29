import boto3
import json
import io

from zipfile import ZipFile, ZipInfo
import pandas as pd
from io import BytesIO
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

from modules.constants import *
from modules.utils.converters import convert_to_utf8
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.helpers import determine_encoding
from modules.utils.readers import read_pdf, read_txt, read_xls, read_secret_aws_secrets

class Carrier:
    def __init__(self, name: str) -> None:
        self.name = name.upper()
        try:
            with open(f"{DBFS_CONFIG}{name.upper()}.json", "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except:
            pass

    def get_file(
        self,
        file_type: str,
        date: str = datetime.now().strftime("%Y%m"),
        max_cols: int = 0,
    ):
        if max_cols:
            result = self._get_files_from_S3(file_type, date, max_cols)
        else:
            result = self._get_files_from_S3(file_type, date)

        return result

    def _get_files_from_S3(
        self, file_type: str, date: str, path: str=BUCKET_PATH
    ) -> tuple[str, BytesIO, list[ZipInfo]]:
        self.file_type = file_type
        try:
            self.path = f"{path}{self.name}_{self.file_type.upper()}_{date}.zip"
            s3_object = self._s3_object(self.path)
            zip_name = f"{self.name}_{self.file_type.upper()}_{date}.zip"

        #This was used for SAS files, we probably can remove it
        except:
            self.path = f"{path}{self.name}_{self.file_type.upper()}.zip"
            s3_object = self._s3_object(self.path)
            zip_name = f"{self.name}_{self.file_type.upper()}.zip"

        zip_file = io.BytesIO(s3_object["Body"].read())

        result = zip_name, zip_file

        return result

    def handle_files(
        self, zip_name: str, zip_data: BytesIO, area_table=None
        ) -> list:
        zip_file = ZipFile(zip_data)
        files = []
        
        for text_file in zip_file.infolist():
            if (text_file.file_size <= 3):
                #empty file
                continue
            elif text_file.filename.lower().endswith( (".txt", ".csv", ".dat")):
                schema = self.file_type
                csv_bytes = zip_file.read(text_file)
                source_encoding = determine_encoding(csv_bytes)                
                self.utf8_encoded_content = convert_to_utf8(
                    csv_bytes, source_encoding
                )
                csv_data = self.utf8_encoded_content.decode("utf-8")
                df = read_txt(self.config, self.file_type, csv_data, schema)
            elif text_file.filename.lower().endswith((".xls", ".xlsx")):
                xls_bytes = zip_file.read(text_file.filename)
                df = read_xls(self.config, self.file_type, xls_bytes)

            elif text_file.filename.lower().endswith(".pdf"):
                pdf_bytes = zip_file.read(text_file.filename)
                df_list = read_pdf(pdf_bytes, area_table)

                if isinstance(df_list, list):
                    try:
                        for df in df_list:
                            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
                            df["ETL_SOURCE_ZIP_NAME"] = zip_name
                            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
                            files.append(df)
                        continue
                    except:
                        continue
                elif isinstance(df_list, pd.DataFrame):
                    try:
                        df_list["ETL_SOURCE_FILE_NAME"] = text_file.filename
                        df_list["ETL_SOURCE_ZIP_NAME"] = zip_name
                        df_list['SOURCE_FILE_NAME_LINE_NUMBER'] = df_list.index +1
                        files.append(df_list)
                        continue
                    except:
                        continue
            else:
                print(f'There is no process for dealing with the current filetype')
                continue
            df = df[df.filter(regex="^(?!Unnamed)").columns]
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)

        return files

    def upload_to_snowflake(self, df: object, run_id: str, date_time: str, data_type=None):
        df["RUN_ID"] = run_id
        df["DATE_TIME"] = date_time
        conn = self._get_snowflake_credentials()

        if data_type:
            table = f"{self.config[self.file_type]['table']}_{data_type}"
        else:
            table = self.config[self.file_type]["table"]
        write_pandas(
            conn,
            df,
            table,
            auto_create_table=True,
            quote_identifiers=False,
        )
        conn.close()

    def _get_secret(self, secret_name):
        secret_name = f"{SECRETS_PATH}/{secret_name}"
        region_name = SECRETS_REGION

        secrets = read_secret_aws_secrets(secret_name, region_name)

        return secrets

    def _s3_object(self, path):
        secret = self._get_secret(AWS_SECRET_NAME)

        access_key = secret["aws_access_key_id"]
        secret_key = secret["secret_key"]
        bucket = BUCKET_NAME

        s3_client = boto3.client(
            "s3",
            region_name=BUCKET_REGION,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        # s3_object = s3_resource.get_object(Bucket=bucket, Key=path)
        s3_object = s3_client.get_object(Bucket=bucket, Key=path)

        return s3_object

    def _get_snowflake_credentials(self):
        credentials = self._get_secret(SNOWFLAKE_SECRET_NAME)

        self.conn = snowflake.connector.connect(
            account=credentials["account"],
            user=credentials["username"],
            password=credentials["password"],
            warehouse=credentials["warehouse"],
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role="EUAG-AWS-APAAS-BRAZILHEALTHANALYTICS-DATAENG",
            authenticator="https://aon.okta.com",
        )

        return self.conn