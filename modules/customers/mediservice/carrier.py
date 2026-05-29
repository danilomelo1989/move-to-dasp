import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8, convert_xls_to_dictionary
from modules.utils.helpers import determine_encoding
from modules.utils.readers import read_first_char_separator, read_text_with_positional_columns, read_txt, read_xls, read_unstr_file

class CarrierMediservice(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("MEDISERVICE")


    def handle_files_managerial(
        self, zip_name: str, zip_data: BytesIO,  max_cols=0
    ) -> list:
        zip_file = ZipFile(zip_data)
        files = []
        for text_file in zip_file.infolist():
            #managerial files from MEDISERVICE are unstructred .xls files
            df = read_unstr_file(self.config, self.file_type, text_file, zip_file, max_cols)
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)
                
                       
        return files
