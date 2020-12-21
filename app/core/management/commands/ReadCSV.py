import boto3  #read csv file from S3 bucket
import pandas as pd

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Django command to read source CSV and print out its content"""

    def handle(self, *args, **options):
        # Let's use Amazon S3
        s3 = boto3.client('s3')

        """TODO: move the path to an environment variable"""
        path ='s3://sibtc.static.1234/Test.csv'
        df =pd.read_csv(path)
        self.stdout.write(df.head())


