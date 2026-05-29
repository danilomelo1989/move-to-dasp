from modules.customers.unimed_curitiba.carrier import CarrierUnimedCuritiba
from modules.carrier import Carrier
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table 
from modules.utils.converters import convert_str

def execute_pipeline(file_type: str, date: str):
    context = Carrier("UNIMED-CURITIBA")
    run_id = generate_run_id()
    date_time = generate_datetime()
    area_table = (120.96, 26, 325.44, 661.68)      
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file, area_table)

    for dataframe in df:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)


def execute_pipeline_registry(file_type: str, date: str):
    context = Carrier("UNIMED-CURITIBA")
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)

    for dataframe in df:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)


def execute_pipeline_claims(file_type: str, date: str):
    context = CarrierUnimedCuritiba()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)

    for data_type, dataframes_list in df.items():
        for dataframe in dataframes_list:
            context.upload_to_snowflake(dataframe, run_id, date_time, data_type)

