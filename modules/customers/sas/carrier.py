import json
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8
from modules.utils.helpers import determine_encoding
from modules.utils.readers import read_txt
from modules.constants import BUCKET_SAS, DBFS_CONFIG
from snowflake.connector.pandas_tools import write_pandas

class CarrierSas(Carrier):
    '''Adaption of class Carrier to transform positional txt file into dataframe'''

    def __init__(self, name: str) -> None:
        self.name = name.upper()
        try:
            with open(f"{DBFS_CONFIG}SAS.json", "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except:
            pass

    def handle_files(self, zip_name: str, zip_file: BytesIO) -> list:
        zip_file = ZipFile(zip_file)
        dataframes = {}

        for text_file in zip_file.infolist():
            file_name_splited = text_file.filename.split("_")
            file_type = [x for x in file_name_splited if "-" in x][0].replace('.txt', '')
            csv_bytes = zip_file.read(text_file)
            source_encoding = determine_encoding(csv_bytes)
            self.utf8_encoded_content = convert_to_utf8(csv_bytes, source_encoding)
            csv_data = self.utf8_encoded_content.decode('utf-8')
            df = read_txt(self.config, file_type, csv_data, file_type)

            df['ETL_SOURCE_FILE_NAME'] = text_file.filename
            df['ETL_SOURCE_ZIP_NAME'] = zip_name
            dataframes[file_type] = df

        return dataframes

    def get_files_from_S3(
        self,
        suffix: str=None,
        path: str=BUCKET_SAS
    ) -> tuple[str, BytesIO, list[ZipFile]]:

        self.path = f"{path}{self.name}/sdc-{self.name}_{suffix}.zip"
        zip_name = f"sdc-{self.name}_{suffix}.zip"

        s3_object = self._s3_object(self.path)
        zip_file = BytesIO(s3_object["Body"].read())

        return zip_name, zip_file
    
    def upload_to_snowflake(self, df: object, run_id: str, date_time: str, data_type=None):
        df["RUN_ID"] = run_id
        df["DATE_TIME"] = date_time
        conn = self._get_snowflake_credentials()

        table = f'{self.name}_{data_type.replace("-", "_")}'
            
        write_pandas(
            conn,
            df,
            table,
            auto_create_table=True,
            quote_identifiers=False,
        )
        conn.close()