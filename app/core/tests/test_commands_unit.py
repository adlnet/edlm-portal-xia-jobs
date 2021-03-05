from unittest.mock import patch

from django.core.management import call_command
from django.test import SimpleTestCase
from django.db.utils import OperationalError
from django.test import tag
import pandas as pd
import logging
from core.management.commands.extract_source_metadata import \
    add_publisher_to_source

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
        data = {
        "LMS": ["Success Factors LMS v. 5953"],
        "XAPI": ["Y"],
        "SCORM": ["N"] }

        test_df = pd.DataFrame.from_dict(data)
        result = add_publisher_to_source(test_df, 'dau')
        key_exist = 'SOURCESYSTEM' in result[0]
        self.assertTrue(key_exist)
