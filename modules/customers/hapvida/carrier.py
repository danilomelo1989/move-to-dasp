import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from modules.utils.converters import convert_to_utf8
from modules.utils.helpers import determine_encoding
from modules.utils.readers import  read_txt, read_xls, read_pdf


class CarrierHapvida(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("HAPVIDA")

    def handle_managerial(
        self, zip_name: str, zip_data: BytesIO
    ) -> list:
        zip_file = ZipFile(zip_data)
        files = []

        for text_file in zip_file.infolist():            
            pdf_bytes = zip_file.read(text_file.filename)
            expected_columns = ['Período', 'Ativos', 'Faturado', 'Co- Participação', 'Cst Assistencial', 'Saldo', 'Sinistralidade']
            try:
                table = read_pdf(pdf_bytes)
                for dataframe in table:
                    if(set(dataframe.columns).issubset(set(expected_columns))):
                        df = dataframe
                        break
                    else:
                        raise ValueError("Dataframe is missing one or more expected columns from general method")
                    
            except ValueError as ve:
                print(ve)
                table_area = (113.76, 88.56, 207.36, 406.8)    
                table = read_pdf(pdf_bytes, table_area)
                for dataframe in table:
                    if(set(dataframe.columns).issubset(set(expected_columns))):
                        df = dataframe
                        break
                    else:
                        raise Exception("Dataframe is missing one or more expected columns from method with area")
                
            df = df[df.filter(regex='^(?!Unnamed)').columns]
            df['ETL_SOURCE_FILE_NAME'] = text_file.filename
            df['ETL_SOURCE_ZIP_NAME'] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            files.append(df)
        
        return files
    
    def _pivot_rows(self, dataframe):
        dataframe['nome_empresa_operadora'] = None
        dataframe['nome_sub_empresa'] = None
        dataframe['descricao_boleto'] = None
        current_level1 = None
        current_level2 = None
        current_level3 = None
        for index, row in dataframe.iterrows():
            value = row[0]            
            if isinstance(value, str):
                if 'Empresa' in value:
                    current_level1 = value
                    current_level2 = None
                if 'Boleto' in value:
                    current_level3 = value  
                elif 'Unidade' in value:
                    current_level2 = value
                elif current_level1:
                    dataframe.at[index, 'nome_empresa_operadora'] = current_level1
                    dataframe.at[index, 'nome_sub_empresa'] = current_level2
                    dataframe.at[index, 'descricao_boleto'] = current_level3
        dataframe.ffill(axis=0, inplace=True)
        dataframe = dataframe[~dataframe['COD_EMP'].str.contains('Empresa|Unidade|Boleto', na=False)]
        
        return dataframe
