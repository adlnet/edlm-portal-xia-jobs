from django.test import SimpleTestCase
from django.test import tag
from core.management.utils.xsr_client import aws_get


@tag('unit')
class UtilsTests(SimpleTestCase):

    def test_aws_get(self):
        """This will test that the bucket name returned is correct"""
        bucket_name = 'dauxsr'
        result_bucket_name, _, _ = aws_get()

        self.assertEqual(bucket_name, result_bucket_name)
