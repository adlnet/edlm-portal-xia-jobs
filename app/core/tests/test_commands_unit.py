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
from core.management.commands.transform_source_metadata import \
    create_target_metadata_dict, replace_field_on_target_schema
from core.management.commands.validate_target_metadata import \
    get_required_recommended_fields_for_target_validation

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class CommandTests(SimpleTestCase):
    """Test cases for waitdb """

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

    """Test cases for extract_source_metadata """

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

    """Test cases for validate_source_metadata """

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

    """Test cases for transform_source_metadata """

    def test_create_target_metadata_dict(self):
        """Test to check transformation of source to target schema and
        replacing None values with empty strings"""
        source_data_dict = {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "OUSD(C)",
            "VENDOR": "Skillsoft",
            "AUDIENCE": "C-Civilian",
            "COURSEID": "IFS0067",
            "ITEMTYPE": "SEMINAR",
            "COURSENAME": "AFOTEC 301 TMS",
            "Unnamed: 19": "Linked to a JKO-hosted course",
            "508COMPLIANT": "y",
            "SOURCESYSTEM": "DAU",
            "SOURCE_FILES": "N",
            "FLASHIMPACTED": "Y",
            "DELIVERYMETHOD": None,
            "CONVERTEDREMAKE": "N",
            "COURSEDESCRIPTION": "course covers the Identify Stakeholders",
            "MANDATORYTRAINING": "N",
            "SUPERVISORMANAGERIAL": "Y",
            "COMMONMILITARYTRAINING": ""
        }

        target_mapping_dict = {
            'Course': {
                'CourseProviderName': 'SOURCESYSTEM',
                'DepartmentName': 'AGENCY',
                'EducationalContext': 'MANDATORYTRAINING',
                'CourseType': 'ITEMTYPE',
                'CourseCode': 'COURSEID',
                'CourseTitle': 'COURSENAME',
                'CourseDescription': 'COURSEDESCRIPTION',
                'CourseAudience': 'AUDIENCE',
                'CourseSectionDeliveryMode': 'DELIVERYMETHOD'
            },
            'Lifecycle': {
                'Provider': 'VENDOR',
                'Maintainer': 'OPR',
                'OtherRole': 'LMS'
            }
        }

        expected_data_dict = {
            0: {
                'Course': {
                    'CourseProviderName': 'DAU',
                    'DepartmentName': 'OUSD(C)',
                    'EducationalContext': 'N',
                    'CourseType': 'SEMINAR',
                    'CourseCode': 'IFS0067',
                    'CourseTitle': 'AFOTEC 301 TMS',
                    'CourseDescription': 'course covers Identify Stakeholders',
                    'CourseAudience': 'C-Civilian',
                    'CourseSectionDeliveryMode': ''
                },
                'Lifecycle': {
                    'Provider': 'Skillsoft',
                    'Maintainer': 'Marine Corps',
                    'OtherRole': 'Success Factors LMS v. 5953'
                }
            }
        }

        result_data_dict = create_target_metadata_dict(target_mapping_dict,
                                                       source_data_dict)

        self.assertEqual(result_data_dict[0]['Course'].get('CourseCode'),
                         expected_data_dict[0]['Course'].get('CourseCode'))
        self.assertEqual(
            result_data_dict[0]['Course'].get('CourseSectionDeliveryMode'),
            expected_data_dict[0]['Course'].get('CourseSectionDeliveryMode'))
        self.assertEqual(result_data_dict[0]['Lifecycle'].get('Maintainer'),
                         expected_data_dict[0]['Lifecycle'].get('Maintainer'))

    def test_replace_field_on_target_schema(self):
        """Test to check Replacing values in field referring target schema"""
        test_data_educational_context_Y = {
            0: {
                'Course': {
                    'EducationalContext': 'y',
                    'CourseCode': 'wd_ango_a05_it_enus',
                },
                'Lifecycle': {
                    'Provider': 'DIA',
                }
            }
        }

        test_data_educational_context_N = {
            0: {
                'Course': {
                    'EducationalContext': 'n',
                    'CourseCode': 'wd_ango_a05_it_enus',
                },
                'Lifecycle': {
                    'Provider': 'DIA',
                }
            }
        }

        expected_data_educational_context_Y = {
            'Course': {
                'EducationalContext': 'Mandatory',
                'CourseCode': 'wd_ango_a05_it_enus',
            },
            'Lifecycle': {
                'Provider': 'DIA',
            }
        }

        expected_data_educational_context_N = {
            'Course': {
                'EducationalContext': 'Non - Mandatory',
                'CourseCode': 'wd_ango_a05_it_enus',
            },
            'Lifecycle': {
                'Provider': 'DIA',
            }
        }

        replace_field_on_target_schema(0, 'Course', 'EducationalContext',
                                       test_data_educational_context_Y)
        replace_field_on_target_schema(0, 'Course', 'EducationalContext',
                                       test_data_educational_context_N)

        self.assertEqual(test_data_educational_context_Y[0]['Course'].get(
            'EducationalContext'),
            expected_data_educational_context_Y['Course'].get(
                'EducationalContext'))
        self.assertEqual(test_data_educational_context_N[0]['Course'].get(
            'EducationalContext'),
            expected_data_educational_context_N['Course'].get(
                'EducationalContext'))

    """Test cases for validate_target_metadata """

    def test_get_required_recommended_fields_for_target_validation(self):
        """Test for Creating list of fields which are Required and
        recommended """
        data = {'Course': {
            'CourseProviderName': 'Required',
            'DepartmentName': 'Required',
            'EducationalContext': 'Recommended',
            'CourseCode': 'Required',
            'CourseTitle': 'Required',
            'CourseDescription': 'Required',
            'CourseAudience': 'Required',
            'CourseSectionDeliveryMode': 'Optional'
        },
            'Lifecycle': {
                'Provider': 'Required',
                'Maintainer': 'Required',
                'OtherRole': 'Optional'
            }
        }

        required_dict = {'Course': ['CourseProviderName', 'DepartmentName',
                                    'CourseCode', 'CourseTitle',
                                    'CourseDescription',
                                    'CourseAudience'],
                         'Lifecycle': ['Provider', 'Maintainer']}
        recommended_dict = {'Course': ['EducationalContext'], 'Lifecycle': []}

        req_dict1, rcm_dict2 = \
            get_required_recommended_fields_for_target_validation(data)
        self.assertEqual(required_dict, req_dict1)
        self.assertEqual(recommended_dict, rcm_dict2)
