from modules.carrier import Carrier
from modules.customers.amil.carrier import CarrierAmil
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table, standardize_table_add_columns



def execute_pipeline(file_type: str, date: str):
    context = Carrier("AMIL")
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)

    for dataframe in df:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)
        
def execute_pipeline_copart( file_type: str, date: str):
    context = CarrierAmil()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    files = context.handle_copart(zip_name, zip_file)

    for data_type, dataframes_list in files.items():
        for dataframe in dataframes_list:
            context.upload_to_snowflake(
                dataframe,
                run_id,
                date_time,
                data_type=data_type
            )

def execute_pipeline_managerial(date: str, file_type: str='GERENCIAL'):
    context = CarrierAmil()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    files = context.handle_managerial(zip_name, zip_file)

    for dataframe in files:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)   
        context.upload_to_snowflake(
            df_snowflake,
            run_id,
            date_time
        )
