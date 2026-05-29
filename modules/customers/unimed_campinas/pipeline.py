from modules.customers.unimed_campinas.carrier import CarrierUnimedCampinas
from modules.carrier import Carrier
from modules.utils.converters import convert_str
from modules.utils.generators import generate_datetime, generate_run_id
from modules.utils.standardizers import standardize_table

def execute_pipeline_managerial(file_type: str, date: str):
    context = CarrierUnimedCampinas()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)
    for data_type, dataframes_list in df.items():
        for dataframe in dataframes_list:
            context.upload_to_snowflake(dataframe, run_id, date_time, data_type)




def execute_pipeline_claims(file_type: str, date: str):
    context = CarrierUnimedCampinas()
    run_id = generate_run_id()
    date_time = generate_datetime()
    zip_name, zip_file = context.get_file(file_type, date)
    df = context.handle_files(zip_name, zip_file)

    for data_type, dataframes_list in df.items():
        for dataframe in dataframes_list:
            context.upload_to_snowflake(dataframe, run_id, date_time, data_type)

