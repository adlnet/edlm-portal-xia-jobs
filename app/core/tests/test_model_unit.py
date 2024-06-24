from core.models import XSRConfiguration
from django.test import TestCase, tag


@tag('unit')
class ModelTests(TestCase):

    def test_create_xsr_configuration(self):
        """Test that creating a new XSR Configuration entry is successful
        with defaults """
        xsr_api_endpoint = 'api/test_file'

        xiaConfig = XSRConfiguration(
            xsr_api_endpoint=xsr_api_endpoint)

        self.assertEqual(xiaConfig.xsr_api_endpoint,
                         xsr_api_endpoint)
