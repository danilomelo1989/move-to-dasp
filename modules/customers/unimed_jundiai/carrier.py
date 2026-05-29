
import zipfile
import fitz
import tabula
from tabula.io import read_pdf
import numpy as np
import io


from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8, convert_xls_to_dictionary
from modules.utils.helpers import determine_encoding
from modules.utils.readers import  read_txt

class CarrierUnimedJundiai(Carrier):
    
    '''Adaption of class Carrier to transform positional txt file into dataframe'''

    def __init__(self):
        super().__init__('UNIMED-JUNDIAI')

    def handle_files(
        self, zip_name: str, zip_data: BytesIO, area_table=None
        ) -> list:
        zip_file = ZipFile(zip_data)
        files = []
        
        for text_file in zip_file.infolist():
            if text_file.filename.lower().endswith( (".txt", ".csv", ".dat")):
                schema = self.file_type
                csv_bytes = zip_file.read(text_file)
                source_encoding = determine_encoding(csv_bytes)                
                self.utf8_encoded_content = convert_to_utf8(
                    csv_bytes, source_encoding
                )
                csv_data = self.utf8_encoded_content.decode("utf-8")
                df = read_txt(self.config, self.file_type, csv_data, schema)

            elif text_file.filename.lower().endswith(".pdf"):
                pdf_bytes = zip_file.read(text_file.filename)
                df_list = read_pdf_managerial(self, pdf_bytes)

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
            df = df[df.filter(regex="^(?!Unnamed)").columns]
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)

        return files
    

def read_pdf_managerial(self, pdf_bytes):
    table_area = (113.76,0.72,357.12,828)
    dataframe = tabula.read_pdf(io.BytesIO(pdf_bytes), pages = 'all')
    dataframe_novo = dataframe[0].shift(1)
    dataframe_novo.iloc[0]= dataframe[0].columns
    num_columns = dataframe_novo.shape[1]
    dataframe_novo.columns = range(1, num_columns + 1)
    area_header = (35.28,0.72,51.84,785.52)
    header_empresa = tabula.read_pdf(io.BytesIO(pdf_bytes), pages = 'all',  area = area_header, stream = True)
    dataframe_novo['0'] = header_empresa[0].columns[0]
    dataframe_novo = dataframe_novo.astype(str)

    return dataframe_novo