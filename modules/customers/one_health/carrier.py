import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8, convert_xls_to_dictionary
from modules.utils.helpers import determine_encoding
from modules.utils.readers import read_first_char_separator, read_text_with_positional_columns, read_txt, read_xls


class CarrierOneHealth(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("ONE-HEALTH")

    def handle_files(
        self, zip_name: str, zip_data: BytesIO
    ) -> list:
        zip_file = ZipFile(zip_data)
        files = []

        for text_file in zip_file.infolist():
            if text_file.filename.lower().endswith(
                ".txt"
            ) or text_file.filename.lower().endswith(".csv"):
                    schema = self.file_type
                    csv_bytes = zip_file.read(text_file)
                    source_encoding = determine_encoding(csv_bytes)
                    self.utf8_encoded_content = convert_to_utf8(
                        csv_bytes, source_encoding
                    )
                    csv_data = self.utf8_encoded_content.decode("utf-8")
                    dataframe = read_txt(self.config, self.file_type, csv_data, schema, None)
                    df = dataframe
            elif text_file.filename.lower().endswith(".xls"):
                df = read_xls(self.config, self.file_type, text_file)
                df = df[df.filter(regex="^(?!Unnamed)").columns]
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)

        return files
    
    def _start_row_manegerial(self, xls_content):
        """Load the Excel file"""
        excel_data = pd.read_excel(xls_content, sheet_name=None)

        """Get the first sheet name"""
        sheet_name = next(iter(excel_data))

        """ Get the data of the first sheet"""
        sheet_data = excel_data[sheet_name]

        """ Find the index of the row with "Receita" in the first 30 columns"""
        start_row = None
        for col_index in range(30):
            column = sheet_data.iloc[:, col_index]
            for index, value in column.items():
                if str(value).strip().lower() == "receita":
                    start_row = index
                    break
            if start_row is not None:
                break

        result = start_row + 1

        return result
    
    def files_to_dictionary(
        self, zip_name: str, zip_data: ZipFile, has_first_char_separator=False
    ) -> dict:
        # Create an empty dictionary to store the dataframes
        dataframes_dict = {}

        """ Reading the .zip infos and taking the .txt file"""
        with zipfile.ZipFile(zip_data, 'r') as zip_file:
            for file_info in zip_file.infolist():
                if file_info.filename.lower().endswith('.txt'):
                    txt_content = zip_file.read(file_info)
                    if has_first_char_separator:
                        dataframes = read_first_char_separator(self.file_type, txt_content, self.config, file_info.filename, zip_name)
                    else:
                        dataframes = read_text_with_positional_columns(self.file_type, txt_content, self.config)

                    for identifier, dataframe in dataframes.items():
                        dataframe['ETL_SOURCE_FILE_NAME'] = file_info.filename
                        dataframe['ETL_SOURCE_ZIP_NAME'] = zip_name
                        dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1                        
                        if identifier not in dataframes_dict:
                            dataframes_dict[identifier] = []
                            dataframes_dict[identifier].append(dataframe) 


                # For .xls files (Manegerial File or Manual Adjustment File)
                elif file_info.filename.lower().endswith('.xlsx'):
                    xls_content = zip_file.open(file_info.filename)
                    start_row = self._start_row_manegerial(xls_content)
                    dataframes = convert_xls_to_dictionary(self.config, self.file_type, xls_content, file_info.filename, zip_name, self.config[self.file_type].get("header", start_row)) 
                    #print (str(dataframes))
                    if "X" not in dataframes_dict:
                        dataframes_dict["X"] = []
                    dataframes_dict["X"].append(dataframes)            

        return dataframes_dict   