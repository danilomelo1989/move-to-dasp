import tabula
import fitz
import zipfile
from modules.carrier import Carrier
import pandas as pd
from zipfile import ZipFile
import io
from io import BytesIO
from modules.utils.readers import read_pdf, read_txt
from modules.utils.converters import convert_to_utf8
from modules.utils.helpers import determine_encoding


class CarrierCNU(Carrier):

    """Adaption of class Carrier to transform positional txt file into dataframe"""

    def __init__(self):
        super().__init__("CENTRAL-NACIONAL-UNIMED")

    def handle_files(
        self, zip_name: str, zip_data: BytesIO, area_table=None
        ) -> list:
        zip_file = ZipFile(zip_data)
        files = []
        
        for text_file in zip_file.infolist():
            if text_file.filename.lower().endswith( (".txt", ".csv")):
                schema = self.file_type
                csv_bytes = zip_file.read(text_file)
                source_encoding = determine_encoding(csv_bytes)                
                self.utf8_encoded_content = convert_to_utf8(
                    csv_bytes, source_encoding
                )
                csv_data = self.utf8_encoded_content.decode("utf-8")
                header = self._header_start(csv_data)
                df = read_txt(self.config, self.file_type, csv_data, schema, header)
            df = df[df.filter(regex="^(?!Unnamed)").columns]
            df["ETL_SOURCE_FILE_NAME"] = text_file.filename
            df["ETL_SOURCE_ZIP_NAME"] = zip_name
            df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
            
            files.append(df)

        return files
            

    def handle_managerial(
        self, zip_name: str, zip_data: BytesIO
    ) -> list:
        zip_file = ZipFile(zip_data)
        files = []

        for text_file in zip_file.infolist():            
            title_area = (28.8, 308.88, 45.36, 536.4)
            pdf_bytes = zip_file.read(text_file)
            file_title = tabula.read_pdf(io.BytesIO(pdf_bytes), pages = '1', area = title_area, multiple_tables=False)
            try:
                title = file_title[0]
            except IndexError:
                title = ''

            if 'Demonstrativo de Custo Operacional' not in title:
                dataframe = self._read_managerial_cnu(pdf_bytes)

            else:
                dataframe = self._read_dm_custo_cnu(pdf_bytes)

            
            if isinstance(dataframe, list):
                try:
                    for df in dataframe:
                        df["ETL_SOURCE_FILE_NAME"] = text_file.filename
                        df["ETL_SOURCE_ZIP_NAME"] = zip_name
                        df['SOURCE_FILE_NAME_LINE_NUMBER'] = df.index +1
                        files.append(df)
                    continue
                except:
                    continue
            elif isinstance(dataframe, pd.DataFrame):
                try:
                    dataframe["ETL_SOURCE_FILE_NAME"] = text_file.filename
                    dataframe["ETL_SOURCE_ZIP_NAME"] = zip_name
                    dataframe['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframe.index +1
                    files.append(dataframe)
                    continue
                except:
                    continue
            break
        
        return files
    
    def _header_start(self, csv_content):        
        search_value = ['TIPO_REGISTRO', 'Numero Contrato']
        header= self._rule_skip_row(csv_content, search_value)
        return header
    
    def _rule_skip_row(self, csv_content, search_value):
        try:                        
            csv_data = pd.read_csv(io.BytesIO(csv_content.encode('utf-8')), nrows=100, header=None, on_bad_lines='skip')            
            for index, row in csv_data.iterrows():
                for field in search_value:
                    if field in str(row):
                        return index                
            return None
        except Exception as e:
            print("An error ocurred when trying to find row index", e)
            return None 
        
    def _read_managerial_cnu(self, pdf_bytes):

        df = tabula.read_pdf(io.BytesIO(pdf_bytes), pages="all", stream=True)

        list_df = []
        for tabela in df:
            try:
                if tabela.iloc[0][1] == 'Frequência Quantidade':
                    dataframe = pd.DataFrame()
                    split_values = tabela['Dados Estatísticos'].astype(str).str.split(' ', expand=True)
                    split_values.columns = [f'Novo_{j+1}' for j in range(split_values.shape[1])]
                    dataframe = pd.concat([tabela.drop(columns=['Dados Estatísticos']), split_values], axis=1)
                    list_df.append(dataframe)
            except Exception as e:
                print(f"Error in generating table and spliting merged columns into new columns: {e}")
                print (dataframe.head(5))
                continue

        list_df_t = []
        for dataframe in list_df:
            dataframe = dataframe.T
            dataframe.columns = dataframe.iloc[0]
            dataframe = dataframe[1:]
            list_df_t.append(dataframe)

        df_h = tabula.io.read_pdf(io.BytesIO(pdf_bytes), pages="all", guess=False, stream=True)

        list_df_h = []
        for tabela_h in df_h:
            try:
                if tabela_h.iloc[0][0] == 'Unidade Controle e Acomp.':
                    valor_header = tabela_h.iloc[3][0]
                    list_df_h.append(valor_header)
            except Exception as e:
                print(f'Error in generating header: {e}')
                continue

        list_df_full = []
        for (data, header) in zip(list_df_t, list_df_h):
            data['header_empresa'] = header
            cols=pd.Series(data.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            # rename the columns with the cols list.
            data.columns=cols
            data.columns = data.columns.astype(str)
            list_df_full.append(data)

        return list_df_full    
        
    def _read_dm_custo_cnu(self, pdf_bytes):

        '''Get pages'''
        pdf_document = fitz.open(stream= io.BytesIO(pdf_bytes), filetype="pdf")
        specific_pages = []
        specific_value = "Total Custo Operacional Contrato"
        for page_number in range (pdf_document.page_count):
            page = pdf_document[page_number]
            text = page.get_text()
            if specific_value in text:
                specific_pages.append(page_number + 1)
        pdf_document.close()

        '''Desired fields areas'''
        area_contract = (54.72, 11.52, 77.04, 277.92)          # Contrato
        area_total_value = (193.68, 483.84, 218.16, 833.76)       # Valor Total Contrato

        '''Building tables'''
        list_contract = self._get_value_from_multiple_pages( pdf_bytes, specific_pages, area_contract)
        list_total_value = self._get_value_from_multiple_pages( pdf_bytes, specific_pages, area_total_value)

        dataframe = pd.DataFrame({'contrato': list_contract, 'valor_total_contrato': list_total_value}) 

        return dataframe
    
    def _get_value_from_multiple_pages(self, pdf_bytes, specific_pages, area):
        list_values = []
        for page_number in specific_pages:
            valor_df = tabula.read_pdf(io.BytesIO(pdf_bytes), pages = page_number, area = area, multiple_tables=True)
            concat= ''
            for elements in valor_df[0]:
                concat = concat+' '+elements
            list_values.append(concat)
        return list_values





    
