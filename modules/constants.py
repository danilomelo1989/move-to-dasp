from modules.utils.readers import read_databricks_variable

AWS_SECRET_NAME = read_databricks_variable("AWS_SECRET_NAME", "aws-svc-account")

BUCKET_NAME = read_databricks_variable("BUCKET_NAME", "prod-data-lake-raw-acia-ie")
BUCKET_PATH = read_databricks_variable("BUCKET_PATH", "brazilhealthanalytics/46c00337-efe7-41f0-84c6-b5968909d880/Outputs/")
BUCKET_REGION = read_databricks_variable("BUCKET_REGION", "eu-west-1")
BUCKET_SAS = read_databricks_variable("BUCKET_SAS", "brazilhealthanalytics/46c00337-efe7-41f0-84c6-b5968909d880/SAS/")

DBFS_CONFIG = read_databricks_variable("DBFS_CONFIG", "/dbfs/FileStore/tables/")

DATABRICKS_SECRET_NAME = read_databricks_variable("DATABRICKS_SECRET_NAME", "databricks-pipeline-auth")
DATABRICKS_URL = read_databricks_variable("DATABRICKS_URL", "https://aon-dasp-eu-west-1-brazilhealthanalytics-workbench.cloud.databricks.com")

SECRETS_REGION = read_databricks_variable("SECRETS_REGION", "eu-west-1")
SECRETS_PATH = read_databricks_variable("SECRETS_PATH", "prod/brazilhealthanalytics/databricks")

SNOWFLAKE_DATABASE = read_databricks_variable("SNOWFLAKE_DATABASE", "NONPROD_BRAZILHEALTHANALYTICS_BRADESCO")
SNOWFLAKE_SCHEMA = read_databricks_variable("SNOWFLAKE_SCHEMA", "REFATORACAO")
SNOWFLAKE_SECRET_NAME = read_databricks_variable("SNOWFLAKE_SECRET_NAME", "snowflake-svc-account")

DEBUG = read_databricks_variable("DEBUG", "False").lower() == 'true'