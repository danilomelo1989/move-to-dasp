import io
import json
import boto3
import tabula
import pandas as pd

from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

def read_pdf(pdf_bytes, table_area= None):
        dataframe = tabula.read_pdf(
            io.BytesIO(pdf_bytes), 
            pages="all", 
            stream=True,
            area=table_area
        )
        return dataframe
    
def  read_first_line(file_type, txt_file, config):
    for line in txt_file.decode("latin-1").splitlines():
        strline = line
        break
    return strline

def read_first_char_separator(file_type, txt_file, config, filename=None, zip_name=None):
    dataframes = {}

    for line in txt_file.decode("latin-1").splitlines():
        try:
            first_char = line[0]
        except:
            continue

        config_filtered = config[file_type]

        if first_char not in config_filtered:
            continue

        if first_char not in dataframes:
            columns = config_filtered[first_char]["columns"]
            columns_dict = {column["new_name"]: [] for column in columns}
            dataframes[first_char] = pd.DataFrame(columns_dict)

        columns = config_filtered[first_char]["columns"]

        if filename and zip_name:
            line = f"{line};{zip_name};{filename}"

        lines = line.split(";")
        values = [str(lines[column["field"]]) for column in columns]

        if len(values) != len(columns):
            print(f"Error: Mismatched columns in line: {line.strip()}")
            continue

        dataframes[first_char].loc[len(dataframes[first_char])] = values

    return dataframes

def read_text_with_positional_columns(file_type, txt_file, config, firstchar=0):

    dataframes = {}

    """ Iterate over the lines of .txt file"""
    for line in txt_file.decode("latin-1").splitlines():
        try:
            if (firstchar is None):
                line_id = "0"
            else:
                line_id = line[firstchar]
        except:
            continue


            """ Filter config"""
        config_filtered = config[file_type]
        """Check if the first character is present in the config"""
        if line_id not in config_filtered:
            continue

        """ Check if a dataframe already exists for the first character"""
        if line_id not in dataframes:
            """Create a new dataframe with the specified columns"""
            columns = config_filtered[line_id]["columns"]
            columns_dict = {column["name"]: [] for column in columns}
            dataframes[line_id] = pd.DataFrame(columns_dict)

        """ Extract the values for each column based on the positions"""
        columns = config_filtered[line_id]["columns"]
        values = [line[column["start"] : column["end"]].strip() for column in columns]
        
        """Get out of the loop if there is mismatched columns"""
        if len(values) != len(columns):
            print(f"Error: Mismatched columns in line: {line.strip()}")
            continue

        """ Add the values to the corresponding dataframe"""
        dataframes[line_id].loc[len(dataframes[line_id])] = values

    """ Dict of dataframes"""
    return dataframes

def read_xls(config, file_type, xls_content, header=None):
        dataframe = pd.read_excel(
            xls_content,
            decimal=config[file_type].get('decimal', ','),
            thousands=config[file_type].get('thousand', None),
            header=config[file_type].get('header', header),
            dtype = str,
            index_col=False
        )
        return dataframe

def read_txt(config, file_type, csv_data, schema, header=None):
        dataframe = pd.read_csv(
            io.BytesIO(csv_data.encode("utf-8")),
            sep=config[schema]["separator"],
            decimal=config[schema].get("decimal", ","),
            thousands=config[schema].get("thousand", None),
            header=config[schema].get("header", header),
            skiprows=config[file_type].get("skiprows", None),
            dtype=str,
            low_memory=False,
            index_col=False,
        )
        return dataframe

def read_unstr_file(config, file_type, text_file, zip_file, max_cols):
        dataframe = pd.read_csv(
            zip_file.open(text_file.filename),
            sep=config[file_type]["separator"],
            encoding=config[file_type]["encoding"],
            decimal=config[file_type].get("decimal", ","),
            thousands=config[file_type].get("thousand", None),
            header=config[file_type].get("header", None),
            skiprows=config[file_type].get("skiprows", None),
            dtype=config[file_type].get("dtype", None),
            names=range(max_cols),
            low_memory=False,
            index_col=False,
        )
        return dataframe

def read_databricks_variable(variable_name: str, default_value: str) -> str:
    spark = SparkSession.builder.getOrCreate()
    dbutils = _get_dbutils(spark)
    try:
        variable = dbutils.widgets.get(variable_name)
    except Py4JJavaError:
        variable = default_value

    return variable

def _get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils
    
def read_secret_aws_secrets(secret_name: str, region_name: str) -> dict:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(get_secret_value_response["SecretString"])

    return secrets