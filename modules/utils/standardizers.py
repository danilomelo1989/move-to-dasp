def standardize_header(config, df, type):
    names = {}
    for field in config[type]["layout"]:
        names[field["field"]] = field["new_name"].upper()
    
    df_rename = df.rename(columns=names)
    return df_rename


def add_missing_columns(config, df, type):
    for field in config[type]["layout"]:
        column = field["new_name"].upper()
        #add the empty column if its not present on dataframe
        if column not in df:
            df[column] = None 
    return df    

def standardize_table(config, df, file_type):
    df = standardize_header(config, df, file_type)
    return df


def standardize_table_add_columns(config, df, file_type):
    df = standardize_header(config, df, file_type)
    df = add_missing_columns(config, df, file_type)
    return df

def standardize_header_multiple_layouts(config, df, type, infile_type):
    names = {}

    for field in config[type]["layout"][infile_type]:
        names[field["field"]] = field["new_name"].upper()        

    df_rename = df.rename(columns=names)
    return df_rename