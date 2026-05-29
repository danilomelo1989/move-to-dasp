import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8, convert_xls_to_dictionary
from modules.utils.helpers import determine_encoding
from modules.utils.readers import read_first_char_separator, read_text_with_positional_columns, read_txt, read_xls

class CarrierUnimedCuritiba(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("UNIMED-CURITIBA")


    def handle_files(
        self, zip_name: str, zip_data: BytesIO, area_table=None , start_row=0
    ) -> list:
        zip_file = ZipFile(zip_data)
        dataframes_dict = {}
        for text_file in zip_file.infolist():
            if (text_file.filename.lower().endswith((".txt", ".csv", ".dat"))):
               
                txt_content = zip_file.read(text_file)
                df_list = read_text_with_positional_columns(self.file_type, txt_content, self.config, 6)

            for identifier, dataframe in df_list.items():
                if identifier not in dataframes_dict:
                    dataframes_dict[identifier] = [] 

                dataframe['ETL_SOURCE_FILE_NAME'] = text_file.filename
                dataframe['ETL_SOURCE_ZIP_NAME'] = zip_name
                dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1                        
                dataframes_dict[identifier].append(dataframe)

        return dataframes_dict

