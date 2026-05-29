from modules.customers.intermedica.carrier import CarrierIntermedica
from modules.carrier import Carrier
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_header_multiple_layouts, standardize_table

def execute_pipeline_registration(file_type: str, date: str):
    context = CarrierIntermedica()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    files = context.handle_registration(zip_name, zip_file)

    for data_type, dataframes_list in files.items():
        for dataframe in dataframes_list:
            df_standart = standardize_header_multiple_layouts(context.config, dataframe, file_type, data_type)
            df_snowflake = convert_str(df_standart)
            context.upload_to_snowflake(
                df_snowflake,
                run_id,
                date_time,
                data_type=data_type
            )


def execute_pipeline_claims(file_type: str, date: str):
    context = CarrierIntermedica()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_claims(zip_name, zip_file)

    for dataframe in df:
        df_standart = standardize_table(context.config, dataframe, file_type)
        df_snowflake = convert_str(df_standart)
        context.upload_to_snowflake(df_snowflake, run_id, date_time)