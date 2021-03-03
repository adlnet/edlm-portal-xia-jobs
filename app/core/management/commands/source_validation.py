import json
import pandas as pd
import numpy as np
import os
import boto3
import logging
from django.core.management.base import BaseCommand
from core.models import XIAConfiguration

logger = logging.getLogger('log_error_records')


class Command(BaseCommand):
    """Django command to extract, transform and load DAU data from given AWS environment"""

    def handle(self, *args, **options):

        s3 = boto3.resource('s3')

        xia_data = XIAConfiguration.objects.get(id=1)

        source_dir_path = os.getcwd()
        source_rename_path = os.path.join(source_dir_path, 'rename.csv')

        # data_file_name = xia_data.data_file_name
        data_file_name = xia_data.data_file_name
        source_validation_schema = xia_data.source_validation_schema
        bucket_name = xia_data.bucket_name

        dau_path = 's3://%s/%s/' % (bucket_name, data_file_name)
        dau_json = s3.Object(bucket_name, source_validation_schema)

        # Creating dictionary from schema
        dau_json_content = dau_json.get()['Body'].read().decode('utf-8')
        data_dict = json.loads(dau_json_content)

        # Extracting DAU Data from S3 bucket"
        csv_dau_df = pd.read_csv(dau_path)

        key_list = list()

        # Getting key list whose Value is 'Required'
        for k, v in data_dict.items():
            if v == 'Required':
                key_list.append(k)

        required_column_name = key_list

        empty_id_list = (np.where(csv_dau_df[required_column_name].isnull().any(axis=1)))[0]
        # logging missing records from source
        for i in empty_id_list:
            logger.error(csv_dau_df.loc[[i]].to_string(index=False))

        csv_dau_df = csv_dau_df.drop(empty_id_list, axis=0)

        csv_dau_df.to_csv(source_rename_path, index=False)

        source_rename_data = open(source_rename_path, 'rb')

        s3.Bucket(bucket_name).put_object(Key=data_file_name, Body=source_rename_data)

        xia_data.save()

        print("Source validation completed")
