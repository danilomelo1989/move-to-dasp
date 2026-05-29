from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.readers import read_text_with_positional_columns, read_xls
class CarrierBradesco(Carrier):
    '''Adaption of class Carrier to transform positional txt file into dataframe'''

    def __init__(self, carrier_name: str='BRADESCO'):
        super().__init__(carrier_name)

    def handle_files(self, zip_name: str, zip_file: BytesIO) -> list:
        dataframes_dict = {}
        unziped_data = ZipFile(zip_file)
        for file_info in unziped_data.infolist():
            txt_content = unziped_data.read(file_info)
            dataframes = read_text_with_positional_columns(
                self.file_type,
                txt_content,
                self.config
            )

            for identifier, dataframe in dataframes.items():
                if identifier not in dataframes_dict:
                    dataframes_dict[identifier] = []                            
                dataframe['ETL_SOURCE_FILE_NAME'] = file_info.filename
                dataframe['ETL_SOURCE_ZIP_NAME'] = zip_name  
                dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1                      
                dataframes_dict[identifier].append(dataframe)

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

