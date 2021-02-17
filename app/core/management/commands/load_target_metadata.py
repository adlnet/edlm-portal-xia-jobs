import logging
import os
from django.core.management.base import BaseCommand
from core.models import MetadataLedger
import requests
from django.core import serializers
from django.utils import timezone

logger = logging.getLogger('dict_config_logger')


class Command(BaseCommand):
    """Django command to extract data in the Experience index Agent (XIA)"""

    def handle(self, *args, **options):
        # defining the api-endpoint
        os.environ['no_proxy'] = '*'
        session = requests.Session()
        session.trust_env = False

        api_endpoint = "http://172.18.0.4:8020/api/metadata-ledger/"

        # data = serializers.serialize('json', MetadataLedger.objects.all(),
        #                              fields=(
        #                                  'metadata_record_uuid',
        #                                  'target_metadata',
        #                                  'target_metadata_hash',
        #                                  'target_metadata_key',
        #                                  'target_metadata_key_hash',
        #                                  'record_lifecycle_status'))

        xis_response = requests.post(url=api_endpoint,
                                     json={
                                            "name": "Ted",
                                            "age": 25,
                                            "occupations": "cook"
                                        })
        print(xis_response.text)
