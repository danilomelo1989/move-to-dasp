import json
import chardet
import functools

import modules.utils.connectors as conn
from modules.constants import DEBUG
from modules.utils.generators import generate_datetime, generate_run_id
from pyspark.sql import functions as F

def determine_encoding(bytes_content):
    result = chardet.detect(bytes_content)
    return result["encoding"]

def rule_skip_row():
    return None

def header(_):
    header = rule_skip_row()
    return header

def execute_itermediate_steps(write):
    def decorator_intermediate(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            execution_plan = func(*args, **kwargs)

            if write is True:
                snow_conn = conn.SnowflakeConnector()
                itermediate_table = execution_plan
                itermediate_table.withColumn("DEBUG_ID", F.lit(generate_run_id()))
                itermediate_table.withColumn("DEBUG_RUN_TIME", F.lit(generate_datetime()))
                snow_conn.save_snowflake_table(itermediate_table, func.__name__, 'DEBUG', 'Overwrite')
            return execution_plan
        return wrapper_decorator
    return decorator_intermediate