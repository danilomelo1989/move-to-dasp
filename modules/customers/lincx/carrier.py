import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8, convert_xls_to_dictionary
from modules.utils.helpers import determine_encoding
from modules.utils.readers import read_first_char_separator, read_text_with_positional_columns, read_txt, read_xls , read_pdf

class CarrierLincx(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("LINCX")


    def handle_files_copay(
        self, zip_name: str, zip_data: BytesIO, area_table=None 
    ) -> list:
        zip_file = ZipFile(zip_data)
        dataframes_dict = {}
        for text_file in zip_file.infolist():
            txt_content = zip_file.read(text_file)
            df_list = read_first_char_separator(self.file_type, txt_content, self.config, text_file.filename,zip_name)
            for identifier, dataframe in df_list.items():
                try:
                    dataframes_dict[identifier].append(dataframe)
                except KeyError:
                    dataframes_dict[identifier] = [dataframe]

                dataframe['ETL_SOURCE_FILE_NAME'] = text_file.filename
                dataframe['ETL_SOURCE_ZIP_NAME'] = zip_name 
                dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1                       
                dataframes_dict[identifier].append(dataframe)

        return dataframes_dict
    
    def handle_files_lincx(
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
                df = read_txt(self.config, self.file_type, csv_data, schema, None)
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
                            dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1  
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
