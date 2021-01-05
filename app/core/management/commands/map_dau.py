import json
import pandas as pd
import boto3
import os
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Django command to list all s3 buckets in the given AWS environment"""

    def handle(self, *args, **options):
        s3 = boto3.resource('s3')
        data_file_name = os.environ['DATA_FILE_NAME']
        schema_file = os.environ['SCHEMA_FILE']
        bucket_name = os.environ['BUCKET_NAME']
        upload_bucket_name = os.environ['UPLOAD_BUCKET_NAME']

        # dau_path = 's3://dauxsr1/CSV_DAU_Consolidated.csv'
        dau_path = 's3://%s/%s/' % (bucket_name,data_file_name)

        # upload_path = '/opt/app/openlxp-xia-dau/core/management/commands/CSV_DAU_Result.csv'

        dirpath = os.getcwd()
        output_path = os.path.join(dirpath, 'output.csv')
        # upload_path = '/opt/app/temp/csv_dau_result.csv'
        # json connect

        # dau_json = s3.Object('dauxsr1', 'schema.json')
        dau_json = s3.Object(bucket_name, schema_file)
        dau_json_content = dau_json.get()['Body'].read().decode('utf-8')
        data_dict = json.loads(dau_json_content)
        # self.stdout.write(data_dict)
        # for key in data_dict:
        #     self.stdout.write(key, '->', data_dict[key])
        #     self.stdout.write(data_dict[key])

        csv_dau_df = pd.read_csv(dau_path)

        csv_schema_df = csv_dau_df.rename(columns=data_dict)

        csv_schema_df.to_csv(output_path,index=False)

        # csv_schema_df.to_csv("CSV_DAU_Result.csv",index=False)
        data = open(output_path,'rb')
        # s3.Bucket('dauxia1').put_object(Key='CSV_DAU_Result.csv',Body =data)
        s3.Bucket(upload_bucket_name).put_object(Key='CSV_DAU_Result.csv',Body =data)





