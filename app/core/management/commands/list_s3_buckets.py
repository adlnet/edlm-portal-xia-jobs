
# List the bucket names
import boto3

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Django command to list all s3 buckets in the given AWS environment"""

    def handle(self, *args, **options):
        #ssl._create_default_https_context = ssl._create_unverified_context
        s3 = boto3.client('s3', verify=False)
        response = s3.list_buckets()

        # Output the bucket names
        self.stdout.write('Existing buckets:')
        for bucket in response['Buckets']:
            self.stdout.write(f'  {bucket["Name"]}')




