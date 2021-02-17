from django.test import SimpleTestCase
from core.models import XIAConfiguration
from core.models import MetadataLedger
from django.test import tag

from core.management.utils.xsr_client import aws_get
from core.management.utils.xsr_client import read_file_path


@tag('unit')
class UtilsTests(SimpleTestCase):

    def test_aws_get(self):
        """This will test that the bucket name returned is correct"""
        bucket_name = 'dauxsr'
        result_bucket_name = aws_get()

        self.assertEqual(bucket_name, result_bucket_name)

    def test_read_file_path(self):
        """This will test that the file path is assembled correctly"""
        file_name = "test_file"
        expected_file_path = "s3://dauxsr/test_file/"
        result_file_path = read_file_path(file_name)

        self.assertEqual(result_file_path, expected_file_path)

