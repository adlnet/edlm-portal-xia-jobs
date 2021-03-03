import json
import pandas as pd
import numpy as np
import boto3
import os
import logging
from django.core.management.base import BaseCommand
from core.models import XIAConfiguration

logger = logging.getLogger('log_error_records')


class Command(BaseCommand):
    """Django command to extract, transform and load DAU data from given AWS environment"""

    def handle(self, *args, **options):

        s3 = boto3.resource('s3')

        xia_data = XIAConfiguration.objects.get(id=1)

        target_file_name = xia_data.target_file_name
        target_validation_schema = xia_data.target_validation_schema
        bucket_name = xia_data.bucket_name

        dau_target_json = s3.Object(bucket_name, target_file_name)
        dau_schema_json = s3.Object(bucket_name, target_validation_schema)

        dau_target_json_content = dau_target_json.get()['Body'].read().decode('utf-8')
        target_data = json.loads(dau_target_json_content)

        # Creating dictionary from schema
        dau_schema_json_content = dau_schema_json.get()['Body'].read().decode('utf-8')
        data_dict = json.loads(dau_schema_json_content)

        required_dict = {}
        recommended_dict = {}

        # Getting key list whose Value is 'Required'
        for k in data_dict:
            required_list = []
            recommended_list = []
            for k1, v1 in data_dict[k].items():
                if v1 == 'Required':
                    required_list.append(k1)
                if v1 == 'Recommended':
                    recommended_list.append(k1)

                required_dict[k] = required_list

                recommended_dict[k] = recommended_list
        empty_id_list = []

        for ind in target_data:
            for column in target_data[ind]:
                required_columns = required_dict[column]
                recommended_columns = recommended_dict[column]
                for key in target_data[ind][column]:
                    if key in required_columns:
                        if not target_data[ind][column][key]:
                            logger.error(target_data[ind][column])
                            empty_id_list.append(ind)
                    if key in recommended_columns:
                        if not target_data[ind][column][key]:
                            logger.info(target_data[ind][column])

        for element in empty_id_list:
            del target_data[element]

        target_dir_path = os.getcwd()
        target_mapped_path = os.path.join(target_dir_path, 'target.json')

        with open(target_mapped_path, 'w') as outfile:
            json.dump(target_data, outfile)

        target_mapped_data = open(target_mapped_path, 'rb')

        s3.Bucket(bucket_name).put_object(Key=target_file_name, Body=target_mapped_data)

        print("Target validation completed")
