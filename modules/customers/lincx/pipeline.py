from modules.customers.lincx.carrier import CarrierLincx
from modules.carrier import Carrier
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table, standardize_table_add_columns

def execute_pipeline(file_type: str, date: str):
    context = CarrierLincx()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files_lincx(zip_name, zip_file)

    for dataframe in df:
        df_standart = standardize_table_add_columns(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)
        
def execute_pipeline_copay(file_type: str, date: str):
    context = CarrierLincx()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files_copay(zip_name, zip_file)

    for data_type, dataframes_list in df.items():
        for dataframe in dataframes_list:
            context.upload_to_snowflake(dataframe, run_id, date_time, data_type)