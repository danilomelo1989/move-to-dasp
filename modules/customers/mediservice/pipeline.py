from modules.customers.mediservice.carrier import CarrierMediservice
from modules.carrier import Carrier
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table
from modules.utils.standardizers import standardize_table_add_columns
def execute_pipeline(file_type: str, date: str):
    context = Carrier("MEDISERVICE")
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)

    for dataframe in df:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)


def execute_pipeline_claims(file_type: str, date: str):
    context = Carrier("MEDISERVICE")
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)

    for dataframe in df:
        df_standart = standardize_table_add_columns(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)


def execute_pipeline_managerial(file_type: str, date: str):
    context = CarrierMediservice()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files_managerial(zip_name, zip_file,9)

    for dataframe in df:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)
