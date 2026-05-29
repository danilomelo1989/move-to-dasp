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
from modules.utils.readers import read_first_char_separator, read_text_with_positional_columns, read_txt, read_xls







class CarrierUnimedCampinas(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("UNIMED-CAMPINAS")


    def handle_files(
        self, zip_name: str, zip_data: BytesIO, area_table=None , start_row=0
    ) -> list:
        zip_file = ZipFile(zip_data)
        dataframes_dict = {}
        for text_file in zip_file.infolist():
            if (text_file.filename.lower().endswith((".txt", ".csv", ".dat"))):
                txt_content = zip_file.read(text_file)
                df_list = read_text_with_positional_columns(self.file_type, txt_content, self.config, None)


                for identifier, dataframe in df_list.items():
                    if identifier not in dataframes_dict:
                        dataframes_dict[identifier] = [] 

                    dataframe['ETL_SOURCE_FILE_NAME'] = text_file.filename
                    dataframe['ETL_SOURCE_ZIP_NAME'] = zip_name                        
                    dataframes_dict[identifier].append(dataframe)


            elif (text_file.filename.lower().endswith((".pdf"))):
                pdf_bytes = zip_file.read(text_file)
                df_list = _read_pdf(self, pdf_bytes, text_file, zip_data)
                if isinstance(df_list, list):
                    try:
                        for df in df_list:
                            #df = df[df.filter(regex='^(?!Unnamed)').columns]
                            df['ETL_SOURCE_FILE_NAME'] = text_file.filename
                            df['ETL_SOURCE_ZIP_NAME'] = zip_name
                            dataframes_dict.append(df)
                        continue
                    except:
                        continue
                elif isinstance(df_list, pd.DataFrame):
                    try:
                        df_list['ETL_SOURCE_FILE_NAME'] = text_file.filename
                        df_list['ETL_SOURCE_ZIP_NAME'] = zip_name
                        dataframes_dict.append(df_list)
                        continue
                    except:
                        continue   




        return dataframes_dict
    
def _read_pdf(self, pdf_bytes, text_file, zip_file):
        '''Get pages'''
        pdf_document = fitz.open(stream= io.BytesIO(pdf_bytes), filetype="pdf")
        specific_value = "Espelho Financeiro - QtdUsu, Custos, Receitas"
        for page_number in range (pdf_document.page_count):
            page = pdf_document[page_number]
            text = page.get_text()
            if specific_value in text:
                specific_page = page_number + 1
                break
            else:
                specific_page = page_number
                
        pdf_document.close()

        title_area = (13.68,180,30.96,599.76)
        
        file_title = tabula.read_pdf(io.BytesIO(pdf_bytes), pages = specific_page, area = title_area, multiple_tables=False)

        try:
            self.title = file_title[0].columns
        except:
            self.title = ''
        
        #print(self.title)

        dataframe = pd.DataFrame()

        if self.title == '':
            table_area = (113.76,0.72,357.12,828)
            dataframe = tabula.read_pdf(io.BytesIO(pdf_bytes), pages=specific_page, stream=True, area = table_area)
            dataframe = dataframe[0]  
            list_1 = dataframe.iloc[0,:].tolist()
            list_2 = dataframe.iloc[1,:].tolist()
            list_3 = dataframe.iloc[2,:].tolist()
            columns = []
            for i in range(len(list_1)):
                columns.append(str(list_1[i]) + "_" + str(list_2[i]) + "_" + str(list_3[i]))
            dataframe.columns=columns
            dataframe = dataframe[3:]


        else:     
            for element in self.title:
                if 'Espelho Financeiro - QtdUsu, Custos, Receitas' in element:
                    table_area = (68.4,0.72,509.76,823.68)
                    dataframe = tabula.read_pdf(io.BytesIO(pdf_bytes), pages=specific_page, stream=True, area = table_area)
                    dataframe = dataframe[0].T
                    dataframe.columns = dataframe.iloc[0]
                    dataframe = dataframe[1:]
                    cols=pd.Series(dataframe.columns)
                    for dup in cols[cols.duplicated()].unique(): 
                        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
                    dataframe.columns=cols
                    dataframe.reset_index(inplace=True)
                    dataframe = dataframe.rename(columns = {'index':'mes_competencia'})
       

        return dataframe    
