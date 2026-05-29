from modules.customers.bradesco.carrier import CarrierBradesco
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table, standardize_table_add_columns

def execute_pipeline(file_type: str, date: str):
    context = CarrierBradesco()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    files = context.handle_files(zip_name, zip_file)

    for data_type, dataframes_list in files.items():
        for dataframe in dataframes_list:
            context.upload_to_snowflake(
                dataframe,
                run_id,
                date_time,
                data_type=data_type
            )

def execute_pipeline_managerial(date: str, file_type: str='GERENCIAL'):
    context = CarrierBradesco()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    files = context.handle_managerial(zip_name, zip_file)

    for dataframe in files:
        dataframe = dataframe.dropna(axis=1, how='all')
        df_snowflake = standardize_table_add_columns(context.config, dataframe, file_type)
        context.upload_to_snowflake(
            df_snowflake,
            run_id,
            date_time
        )