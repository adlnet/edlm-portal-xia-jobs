from unittest.mock import patch
from django.core.management import call_command
from django.test import SimpleTestCase
from django.db.utils import OperationalError
from django.test import tag
import pandas as pd
import logging
from core.management.commands.extract_source_metadata import \
    add_publisher_to_source
from core.management.commands.validate_source_metadata import \
    get_required_fields_for_source_validation

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class CommandTests(SimpleTestCase):

    def test_wait_for_db_ready(self):
        """Test that waiting for db when db is available"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.return_value = True
            call_command('waitdb')
            self.assertEqual(gi.call_count, 1)

    @patch('time.sleep', return_value=True)
    def test_wait_for_db(self, ts):
        """Test waiting for db"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.side_effect = [OperationalError] * 5 + [True]
            call_command('waitdb')
            self.assertEqual(gi.ensure_connection.call_count, 6)

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        data = {
            "LMS": ["Success Factors LMS v. 5953"],
            "XAPI": ["Y"],
            "SCORM": ["N"]}

        test_df = pd.DataFrame.from_dict(data)
        result = add_publisher_to_source(test_df, 'dau')
        key_exist = 'SOURCESYSTEM' in result[0]
        self.assertTrue(key_exist)

    def test_get_required_fields_for_source_validation(self):
        """Test for Creating list of fields which are Required"""
        data = {
            "SOURCESYSTEM": "Required",
            "AGENCY": "Required",
            "FLASHIMPACTED": "Optional",
            "SUPERVISORMANAGERIAL": "Optional",
            "ITEMTYPE": "Optional",
            "COURSEID": "Required",
            "COURSENAME": "Optional"
        }
        required_list = ['SOURCESYSTEM', 'AGENCY', 'COURSEID']

        received_list = get_required_fields_for_source_validation(data)
        self.assertEqual(required_list, received_list)
