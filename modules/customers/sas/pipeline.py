from modules.customers.sas.carrier import CarrierSas
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table

def execute_pipeline(carrier_name: str, suffix: str):
    context = CarrierSas(carrier_name)
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_files_from_S3(suffix)
    files = context.handle_files(zip_name, zip_file)

    for data_type, dataframe in files.items():
        df_standart = standardize_table(context.config, dataframe, data_type)
        context.upload_to_snowflake(
            df_standart,
            run_id,
            date_time,
            data_type=data_type
        )
            
