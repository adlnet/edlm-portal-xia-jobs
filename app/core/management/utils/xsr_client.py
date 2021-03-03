import boto3
import json
import logging
import pandas as pd
import os

logger = logging.getLogger('dict_config_logger')


def aws_get():
    bucket_name = 'dauxsr'
    source_file_name = 'DAU_Consolidated.csv'
    source_schema = 'DAU_Source_Renaming_Schema.json'
    return bucket_name, source_file_name, source_schema


def read_source_file():
    """setting file path from s3 bucket"""
    bucket_name, source_file_name, source_schema = aws_get()
    source_file_path = 's3://%s/%s/' % (bucket_name, source_file_name)
    source_data_dict = read_json_data(source_schema)

    logger.info("Retrieving data from XSR")
    source_df = pd.read_csv(source_file_path)
    logger.info("Renaming column values of Data from source")
    std_replaced_nan_df = source_df.where(pd.notnull(source_df),
                                          None)
    std_source_df = std_replaced_nan_df.rename(columns=source_data_dict)

    return std_source_df


def read_json_data(file_name):
    """setting file path for json files and ingesting as dictionary values """
    s3 = boto3.resource('s3')
    bucket_name, x, y = aws_get()

    json_path = s3.Object(bucket_name, file_name)
    json_content = json_path.get()['Body'].read().decode('utf-8')
    data_dict = json.loads(json_content)
    return data_dict


def get_api_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    api_endpoint = os.environ.get('API_ENDPOINT')
    return api_endpoint



