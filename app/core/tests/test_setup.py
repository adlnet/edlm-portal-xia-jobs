import pandas as pd
from django.test import TestCase


class TestSetUp(TestCase):
    """Class with setup and teardown for tests in XIS"""

    def setUp(self):
        """Function to set up necessary data for testing"""

        # globally accessible data sets

        self.source_metadata = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "test name",
            "Start_date": "2017-03-28T00:00:00-04:00",
            "crs_header": "TestData 123",
            "SOURCESYSTEM": "DAU",
            "test_description": "test description",
        }

        self.key_value = "TestData 123_DAU"
        self.key_value_hash = "6acf7689ea81a1f792e7668a23b1acf5"
        self.hash_value = "29f781517cba9c121d6b677803069beb"

        self.test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}

        self.metadata_df = pd.DataFrame.from_dict({1: self.source_metadata},
                                                  orient='index')

        self.xis_api_endpoint_url = 'http://example'
        self.xsr_api_endpoint_url = 'http://example'

        return super().setUp()

    def tearDown(self):
        return super().tearDown()
