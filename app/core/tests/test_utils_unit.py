import hashlib
import logging
from datetime import datetime
from unittest.mock import patch

from core.management.utils.xsr_client import (convert_html,
                                              convert_int_to_date,
                                              custom_moodle_fields, find_dates,
                                              find_html,
                                              get_source_metadata_key_value,
                                              get_xsr_api_endpoint,
                                              get_xsr_api_response,
                                              listToString, read_source_file)
from ddt import data, ddt, unpack
from django.test import tag

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class UtilsTests(TestSetUp):
    """Unit Test cases for utils """

    # Test cases for XSR_CLIENT
    def test_get_xsr_api_endpoint(self):
        """Test to check if API endpoint is present"""
        api_end, tk = get_xsr_api_endpoint(self.xsrConfig)
        self.assertEqual(self.xsr_api_endpoint_url, api_end)
        self.assertEqual(self.token, tk)

    def test_get_xsr_api_response(self):
        """Test to Function to get api response from xsr endpoint"""
        with patch('core.management.utils.xsr_client.get_xsr_api_endpoint') \
                as xsr_ep, patch('requests.get') as response_obj:
            xsr_ep.return_value = self.xsr_api_endpoint_url, self.token
            response_obj.return_value = response_obj

            result_xsr_api_response = get_xsr_api_response(self.xsrConfig)
            self.assertTrue(result_xsr_api_response)

    @patch('core.management.utils.xsr_client.extract_source',
           return_value=dict({1: {'a': 'b'}}))
    def test_read_source_file(self, extract):
        """test to check if data is present for extraction """

        result_data = read_source_file(self.xsrConfig)
        self.assertIsInstance(result_data, list)

    def test_listToString(self):
        converted_string = listToString('[1, 2, 3, 4]')

        self.assertTrue(isinstance(converted_string, str))

    def test_find_html(self):

        html_dict = {'html_code': '<h1>This is heading 1</h1>'}
        converted_html_dict = find_html(html_dict)
        self.assertTrue(isinstance(converted_html_dict['html_code'], str))

    def test_find_dates(self):

        date_int = {'date': 1718050925}
        date_dict = find_dates(date_int)
        self.assertTrue(isinstance(date_dict['date'], datetime))

    def test_convert_html(self):

        html_dict = {'html_code': '<h1>This is heading 1</h1>'}
        convert_html('html_code', html_dict)
        self.assertTrue(isinstance(html_dict['html_code'], str))

    def test_convert_int_to_date(self):

        date_int = {'date': 1718050925}
        convert_int_to_date('date', date_int)
        self.assertTrue(isinstance(date_int['date'], datetime))

    @patch('core.management.utils.xsr_client.get_xsr_api_endpoint',
           return_value=('xsr_url', 'token'))
    def test_custom_moodle_fields(self, ret):

        Val = custom_moodle_fields([self.source_metadata], self.xsrConfig)
        self.assertFalse(Val)

    @patch('core.management.utils.xsr_client.get_xsr_api_endpoint',
           return_value=('xsr_url', 'token'))
    def test_custom_moodle_fields_true(self, ret):

        source_dict = self.source_metadata
        source_dict.update({"categoryname": "ecc approved"})

        Val = custom_moodle_fields([source_dict], self.xsrConfig)
        self.assertTrue(Val)

    @data(('key_field1', 'key_field2'), ('key_field11', 'key_field22'))
    @unpack
    def test_get_source_metadata_key_value(self, first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'idnumber': first_value,
            'SOURCESYSTEM': second_value
        }

        expected_key = first_value + '_' + second_value
        expected_key_hash = hashlib.sha512(expected_key.encode('utf-8')). \
            hexdigest()

        result_key_dict = get_source_metadata_key_value(test_dict)
        self.assertEqual(result_key_dict['key_value'], expected_key)
        self.assertEqual(result_key_dict['key_value_hash'], expected_key_hash)

    @data(('key_field1', ''))
    @unpack
    def test_get_source_metadata_key_value_fail(self,
                                                first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'idnumber': first_value,
            'SOURCESYSTEM': second_value
        }

        result_key_dict = get_source_metadata_key_value(test_dict)

        self.assertEqual(result_key_dict, None)
