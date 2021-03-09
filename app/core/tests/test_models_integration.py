from django.test import TestCase
from core.models import XIAConfiguration
from django.core.exceptions import ValidationError


class ModelTests(TestCase):

    def test_create_two_xia_configuration(self):
        """Test that trying to create more than one XIA Configuration throws
        ValidationError """
        with self.assertRaises(ValidationError):
            xiaConfig = XIAConfiguration(bucket_name="sample_bucket_name",
                                         upload_bucket_name=
                                         "sample_upload_bucket_name")
            xiaConfig2 = XIAConfiguration(bucket_name="sample_bucket_name2",
                                          upload_bucket_name=
                                          "sample_upload_bucket_name2")
            xiaConfig.save()
            xiaConfig2.save()

