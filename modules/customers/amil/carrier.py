from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.readers import read_first_char_separator, read_xls
class CarrierAmil(Carrier):
    '''Adaption of class Carrier to transform positional txt file into dataframe'''
    def __init__(self):
        super().__init__("AMIL")


    def handle_copart(self, zip_name: str, zip_file: BytesIO) -> list:
        dataframes_dict = {}
        unziped_data = ZipFile(zip_file)

        for file_info in unziped_data.infolist():
            txt_content = unziped_data.read(file_info)
            dataframes = read_first_char_separator(
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
            start_row, end_row = self._find_start_row_manegerial(xls_content)
            
            full_file = read_xls(
                self.config,
                self.file_type,
                xls_content
            )
            

            managerial_content = full_file.loc[start_row:end_row]
            managerial_content.columns = range(managerial_content.columns.size)
            managerial_content = managerial_content.dropna(axis=1, how='all')
            
            managerial_content['ETL_SOURCE_FILE_NAME'] = file_info.filename
            managerial_content['ETL_SOURCE_ZIP_NAME'] = zip_name
            managerial_content['SOURCE_FILE_NAME_LINE_NUMBER'] = managerial_content.index +1         




            dataframes_dict.append(managerial_content)


        return dataframes_dict 


    def _find_start_row_manegerial(self, xls_content):
        excel_data = pd.read_excel(xls_content, sheet_name=None)
        sheet_name = next(iter(excel_data))
        sheet_data = excel_data[sheet_name]

        start_row = None
        end_row = None
        for col_index in range(100):
            try:
                column = sheet_data.iloc[:, col_index]
            except IndexError:
                break
            for index, value in column.items():
                if str(value).strip().lower() == 'data de competência':
                    start_row = index
                elif  str(value).strip().lower() == 'total':
                    end_row = index

        return start_row + 1, end_row + 1
