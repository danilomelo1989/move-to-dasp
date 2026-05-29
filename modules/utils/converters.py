import pandas as pd

def convert_str(df):
        df = df.astype(str)
        return df

def convert_to_utf8(bytes_content, source_encoding):
        if source_encoding is None:
            source_encoding = "latin1"
        content = bytes_content.decode(source_encoding)
        utf8_encoded = content.encode("utf-8")
        return utf8_encoded

def convert_xls_to_dictionary(config, file_type, xls_content, filename, zipname=None, header=None):
        dataframes = pd.read_excel(
            xls_content,
            decimal=config[file_type].get("decimal", ","),
            thousands=config[file_type].get("thousand", None),
            header=header,
            dtype=str,
            index_col=False,
        )
        dataframes = dataframes[dataframes.filter(regex="^(?!Unnamed)").columns]
        dataframes["ETL_SOURCE_FILE_NAME"] = filename
        if zipname is not None:
            dataframes["ETL_SOURCE_ZIP_NAME"] = zipname
        dataframes['SOURCE_FILE_NAME_LINE_NUMBER'] = dataframes.index +1 
        return dataframes    

def convert_to_utf8_multiple_test(bytes_content, source_encoding):
        try:
            content = bytes_content.decode(source_encoding)
        except UnicodeDecodeError:              
            try:
                source_encoding = 'latin1'
                content = bytes_content.decode(source_encoding)
            except:
                try:
                    source_encoding = 'cp850'
                    content = bytes_content.decode(source_encoding)
                except:
                    try:
                        source_encoding = 'utf-8-sig'
                        content = bytes_content.decode(source_encoding)
                    except:
                        try:
                            source_encoding = 'ISO-8859-1'
                            content = bytes_content.decode(source_encoding)
                        except:
                            pass
        utf8_encoded = content.encode('utf-8')
        return utf8_encoded