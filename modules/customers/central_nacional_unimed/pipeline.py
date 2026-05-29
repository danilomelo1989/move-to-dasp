from modules.customers.central_nacional_unimed.carrier import CarrierCNU
from modules.carrier import Carrier
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table

def execute_pipeline(file_type: str, date: str):
    context = CarrierCNU()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)
    print(df)

    for dataframe in df:
        try:
            df_standart = standardize_table(context.config, dataframe, file_type)
            df_snowflake = convert_str(df_standart)
            context.upload_to_snowflake(df_snowflake, run_id, date_time)
        except Exception as e:
            print(f"An error ocurred when trying to load file: {dataframe['ETL_SOURCE_FILE_NAME'][0]} \n")
            print("detailed error: ", e)
            print(df_snowflake.columns)
            print(dataframe.columns)
            continue

def execute_pipeline_managerial(file_type: str, date: str):
    context = CarrierCNU()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_managerial(zip_name, zip_file)
    for dataframe in df:
        try:
            df_standart = standardize_table(context.config, dataframe, file_type)
            df_snowflake = convert_str(df_standart)
            context.upload_to_snowflake(df_snowflake, run_id, date_time)
        except Exception as e:
            print(f"An error ocurred when trying to load file: {dataframe['ETL_SOURCE_FILE_NAME'][0]} \n")
            print("detailed error: ", e)
            print(df_snowflake.columns)
            print(dataframe.columns)
            continue


