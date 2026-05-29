# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install chardet
# MAGIC %pip install types-chardet
# MAGIC %pip install tabula-py
# MAGIC %pip install PyMuPDF tabula-py
# MAGIC %pip install openpyxl

# COMMAND ----------

import boto3
import base64
import requests
import pandas as pd

from modules.constants import *
from modules.utils.readers import read_secret_aws_secrets
from modules.utils.connectors import SnowflakeConnector

# COMMAND ----------

#TODO Transform this in a Snowflake table
DEFAULT_JOB = 818520407789917
JOBS = {
    'SULAMERICA': 818520407789917,
    #'AMIL': 504946631032665,
    #'BRADESCO': 1016591368041700,
    #'SULAMERICA': 818520407789917,
    #'UNIMED-CURITIBA': 86293434639446
}

# COMMAND ----------

def run_databrikcs_job(job_parameteres: dict, job_id: int):
    secret = read_secret_aws_secrets(
        f'{SECRETS_PATH}/{DATABRICKS_SECRET_NAME}',
        SECRETS_REGION
    )
    token = secret['token']
    bearer_auth = f'Bearer {token}'

    headers = {
        'Authorization': bearer_auth
    }

    params = {
        'job_id': job_id,
        'job_parameters':  job_parameteres
    }

    request = requests.post(
        f"{DATABRICKS_URL}/api/2.1/jobs/run-now",
        headers=headers,
        json=params
    )

    return request

def get_s3_client():
    secret = read_secret_aws_secrets(
        f'{SECRETS_PATH}/{AWS_SECRET_NAME}',
        SECRETS_REGION
    )

    access_key = secret["aws_access_key_id"]
    secret_key = secret["secret_key"]

    s3_client = boto3.client(
        "s3",
        region_name=BUCKET_REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    return s3_client

def get_s3_metadata(path: str=BUCKET_PATH):
    s3_client = get_s3_client()
    
    files_metadata = []
    s3_bucket_metadata = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=path
    )
    files_metadata += s3_bucket_metadata.get('Contents')
    continuation_token = s3_bucket_metadata.get('NextContinuationToken', None)

    while continuation_token is not None:
        s3_bucket_metadata = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            ContinuationToken=continuation_token,
            Prefix=path
        )
        files_metadata += s3_bucket_metadata.get('Contents')
        continuation_token = s3_bucket_metadata.get('NextContinuationToken', None)

    return files_metadata

def generate_dataframe_from_metadata(metadata: list) -> object:
    files = []
    for file in metadata:
        row = (file['Key'].split('/')[-1], file['LastModified'])
        files.append(row)
        
    header = ['file_name', 'modified_date']
    df_files = spark.createDataFrame(files).toDF(*header)

    return df_files

def trigger_pipelines(table):
    try:
        print("file_name.show(")
        file_name = table.file_name.split('-')[0]
        file_name = file_name.replace('.zip', '')
        
        carrier = file_name.split('_')[0]
        file_type = file_name.split('_')[1]
        date = file_name.split('_')[2]

        print (str(carrier))
        print (str(file_type))
        print (str(date))
    except:
        return


    job_id = JOBS.get(carrier)
    if job_id:
        params = {
            'file_type': file_type,
            'date': date
        }
        run_databrikcs_job(params, job_id)

    return

# COMMAND ----------

metadata = get_s3_metadata()
files = generate_dataframe_from_metadata(metadata)

connector = SnowflakeConnector()
snowflake_table = connector.get_table_from_snowflake('S3_FILES', 'CONTROLE')

files.join(snowflake_table, on='FILE_NAME', how='LEFT'). \
    filter((files.modified_date != snowflake_table.MODIFIED_DATE) | (snowflake_table.MODIFIED_DATE.isNull())).\
    select('file_name').\
    foreach(trigger_pipelines)


#connector.upload_table_to_snowflake(files, 'S3_FILES', 'CONTROLE', 'Overwrite')

