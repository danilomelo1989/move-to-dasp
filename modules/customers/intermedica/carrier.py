import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
import io
from io import BytesIO
from modules.utils.readers import read_txt
from modules.utils.converters import convert_to_utf8_multiple_test
from modules.utils.helpers import determine_encoding

class CarrierIntermedica(Carrier):

    def __init__(self, carrier_name: str='INTERMEDICA'):
        super().__init__(carrier_name)
    
    def handle_claims(
        self, zip_name: str, zip_data: BytesIO, area_table=None
        ) -> list:
        zip_file = ZipFile(zip_data)
        files = []
        
        for text_file in zip_file.infolist():
            if text_file.filename.lower().endswith( (".txt", ".csv")):
                schema = self.file_type
                csv_bytes = zip_file.read(text_file)
                source_encoding = determine_encoding(csv_bytes)                
                self.utf8_encoded_content = convert_to_utf8_multiple_test(
                    csv_bytes, source_encoding
                )
                csv_data = self.utf8_encoded_content.decode("utf-8")
                df = read_txt(self.config, self.file_type, csv_data, schema)
            else:
                print(f'There is no current process for dealing with the file extension: {text_file.filename}')
                continue
            df = df[df.filter(regex="^(?!Unnamed)").columns]
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            
            files.append(df)

        return files

    def handle_registration(self, zip_name: str, zip_data: BytesIO):
        dataframes_dict = {}
        zip_file = ZipFile(zip_data)
        for text_file in zip_file.infolist():
            if text_file.filename.lower().endswith( (".txt", ".csv")):
                schema = self.file_type
                csv_bytes = zip_file.read(text_file)
                source_encoding = determine_encoding(csv_bytes)                
                self.utf8_encoded_content = convert_to_utf8_multiple_test(
                    csv_bytes, source_encoding
                )
                csv_data = self.utf8_encoded_content.decode("utf-8")
                dataframe = read_txt(self.config, self.file_type, csv_data, schema)

                dataframe_type = self._check_dataframe_type(text_file)

                if dataframe_type not in dataframes_dict:
                    dataframes_dict[dataframe_type] = []    

                dataframe['ETL_SOURCE_FILE_NAME'] = text_file.filename
                dataframe['ETL_SOURCE_ZIP_NAME'] = zip_name  
                dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1   
                                
                dataframes_dict[dataframe_type].append(dataframe)
            else:
                print(f'There is no current process for dealing with the file extension: {text_file.filename}')
                continue

        return dataframes_dict 

    def handle_managerial(self, zip_name: str, zip_file: BytesIO) -> list:
        dataframes_dict = []
        unziped_data = ZipFile(zip_file)

        for file_info in unziped_data.infolist():
            xls_content = unziped_data.open(file_info.filename)
            start_row = self._find_start_row_manegerial(xls_content)
            
            managerial_content = read_xls(
                self.config,
                self.file_type,
                xls_content,
                header=start_row
            )

            managerial_content['ETL_SOURCE_FILE_NAME'] = file_info.filename
            managerial_content['ETL_SOURCE_ZIP_NAME'] = zip_name
            managerial_content['SOURCE_FILE_NAME_LINE_NUMBER'] = managerial_content.index +1

            dataframes_dict.append(managerial_content)

        return dataframes_dict 
    
    def _check_dataframe_type(self, csv_content):
        if csv_content.filename.lower().endswith('cadastro_de_associados.csv'):
            dataframe_id = 'cadastro_de_associados'
        elif csv_content.filename.lower().endswith('comissão.csv'):
            dataframe_id = 'comissao'
        elif csv_content.filename.lower().endswith('estado_civil.csv'):
            dataframe_id = 'estado_civil'
        elif csv_content.filename.lower().endswith('redutor_de_custos.csv'):
            dataframe_id = 'redutor_de_custos'
        elif csv_content.filename.lower().endswith('tipo_de_participante.csv'):
            dataframe_id = 'tipo_de_participante'

        return dataframe_id
        
    # Function to find the first row of Bradesco manegerial file
    def _find_start_row_manegerial(self, xls_content):
        excel_data = pd.read_excel(xls_content, sheet_name=None)
        sheet_name = next(iter(excel_data))
        sheet_data = excel_data[sheet_name]

        start_row = None
        for col_index in range(30):
            column = sheet_data.iloc[:, col_index]
            for index, value in column.items():
                if str(value).strip().lower() == 'faturamento':
                    start_row = index
                    break
            if start_row is not None:
                break

        return start_row + 1  