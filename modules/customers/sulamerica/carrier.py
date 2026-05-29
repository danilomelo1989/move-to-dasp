from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.readers import read_txt , read_unstr_file, read_first_line
from modules.utils.converters import convert_to_utf8
from modules.utils.helpers import determine_encoding

class CarrierSulAmerica(Carrier):
    def __init__(self, carrier_name='SULAMERICA'):
        super().__init__(carrier_name)

    def handle_managerial(self, zip_name: str, zip_file: BytesIO) -> list:
        files = []
        unziped_data = ZipFile(zip_file)

        for file_info in unziped_data.infolist():
            schema = self.file_type
            csv_bytes = unziped_data.read(file_info)
            source_encoding = determine_encoding(csv_bytes)
            self.utf8_encoded_content = convert_to_utf8(
                csv_bytes, source_encoding
            )
            csv_data = self.utf8_encoded_content.decode("utf-8")


            managerial_content = read_txt(self.config, self.file_type, csv_data, schema)

            managerial_content = managerial_content.T.iloc[1:].dropna(axis=1, how='all')
            managerial_content.columns = range(managerial_content.columns.size)
            managerial_content['ETL_SOURCE_FILE_NAME'] = file_info.filename
            managerial_content['ETL_SOURCE_ZIP_NAME'] = zip_name
            managerial_content['SOURCE_FILE_NAME_LINE_NUMBER'] = managerial_content.index +1
            print("SOURCE_FILE_NAME_LINE_NUMBER")
            #print(managerial_content)
            files.append(managerial_content)

        return files

    def handle_files_managerial(
        self, zip_name: str, zip_data: BytesIO,  max_cols=0
    ) -> list:
        zip_file = ZipFile(zip_data)
        files = []
        for text_file in zip_file.infolist():
            df = read_unstr_file(self.config, self.file_type, text_file, zip_file, max_cols)
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)
                
                       
        return files
    

    def handle_files_copart(
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

                strline = read_first_line(self.file_type, csv_bytes, self.config)

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

                            df["ANO_MES_COPARTICIPACAO"] = strline
                            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
                            df["ETL_SOURCE_ZIP_NAME"] = zip_name
                            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
                            files.append(df)
                        continue
                    except:
                        continue
                elif isinstance(df_list, pd.DataFrame):
                    try:

                        df_list["ANO_MES_COPARTICIPACAO"] = strline
                        df_list["ETL_SOURCE_FILE_NAME"] = text_file.filename
                        df_list["ETL_SOURCE_ZIP_NAME"] = zip_name
                        df_list['SOURCE_FILE_NAME_LINE_NUMBER'] = df_list.index +1
                        files.append(df_list)
                        continue
                    except:
                        continue
            df = df[df.filter(regex="^(?!Unnamed)").columns]

            df["ANO_MES_COPARTICIPACAO"] = strline            
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)

        return files









    def handle_files_copartcccccc(
    self, zip_name: str, zip_data: BytesIO
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

                strline = read_first_line(self.file_type, csv_bytes, self.config)
                print (strline)

                df = read_txt(self.config, self.file_type, csv_data, schema)
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