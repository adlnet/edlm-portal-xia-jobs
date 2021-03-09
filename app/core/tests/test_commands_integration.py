from django.test import TestCase
from core.models import XIAConfiguration, MetadataLedger
from django.test import tag
from core.management.commands.extract_source_metadata import \
    get_publisher_detail, extract_metadata_using_key
from core.management.commands.validate_source_metadata import \
    get_source_validation_schema, validate_source_using_key, \
    get_source_metadata_for_validation
import logging
from core.management.utils.xsr_client import read_json_data
from core.management.commands.validate_target_metadata import \
                get_target_validation_schema

logger = logging.getLogger('dict_config_logger')


@tag('integration')
class Command(TestCase):

    def test_get_publisher_detail(self):
        """Test that get publisher name from XIAConfiguration """

        xiaConfig = XIAConfiguration(publisher='dau')
        xiaConfig.save()
        result_publisher = get_publisher_detail()
        self.assertEqual('dau', result_publisher)

    def test_extract_metadata_using_key(self):
        """Test if the keys and hash is getting created and saving in
        Metadata_ledger table """

        test_data = {'0': {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "OUSD(C) ",
            "VENDOR": "Skillsoft",
            "AUDIENCE": "C-Civilian",
            "COURSEID": "IFS0067",
            "ITEMTYPE": "SEMINAR",
            "COURSENAME": "AFOTEC 301 TMS",
            "Unnamed: 19": "Linked to a JKO-hosted course",
            "508COMPLIANT": "y",
            "SOURCESYSTEM": "DAU",
            "SOURCE_FILES": " N",
            "FLASHIMPACTED": "Y",
            "DELIVERYMETHOD": "",
            "CONVERTEDREMAKE": "N",
            "COURSEDESCRIPTION": "course covers Identify Stakeholders ",
            "MANDATORYTRAINING": "N",
            "SUPERVISORMANAGERIAL": "Y",
            "COMMONMILITARYTRAINING": ""
        }}

        expected_data = {
            "metadata_record_inactivation_date": "",
            "record_lifecycle_status": "Active",
            "source_metadata": {
                "LMS": "Success Factors LMS v. 5953",
                "OPR": "Marine Corps",
                "XAPI": "Y",
                "SCORM": "N",
                "AGENCY": "OUSD(C) ",
                "VENDOR": "Skillsoft",
                "AUDIENCE": "C-Civilian",
                "COURSEID": "IFS0067",
                "ITEMTYPE": "SEMINAR",
                "COURSENAME": "AFOTEC 301 TMS",
                "Unnamed: 19": "Linked to a JKO-hosted course",
                "508COMPLIANT": "y",
                "SOURCE_FILES": " N",
                "FLASHIMPACTED": "Y",
                "DELIVERYMETHOD": "",
                "CONVERTEDREMAKE": "N",
                "COURSEDESCRIPTION": "course covers the Identify Stakeholders",
                "MANDATORYTRAINING": "N",
                "SUPERVISORMANAGERIAL": "Y",
                "COMMONMILITARYTRAINING": ""
            },
            "source_metadata_hash": "dd83f85b503e052a3997ac945ccdfe02",
        }
        extract_metadata_using_key(test_data)
        result_data = MetadataLedger.objects.values('source_metadata').filter(
            source_metadata_key='DAU_IFS0067').first()
        a = result_data.get('source_metadata')

        self.assertEqual(expected_data['source_metadata'].get('LMS'), a['LMS'])
        self.assertEqual(expected_data['source_metadata'].get('OPR'), a['OPR'])
        self.assertEqual(expected_data['source_metadata'].get('FLASHIMPACTED'),
                         a['FLASHIMPACTED'])

    def test_extract_metadata_using_key_validation(self):
        """Test if the entry is already existing in Metadata Ledger then
        can first record get inactivated or second entry is created in
        Metadata Ledger or not."""

        entry_1 = {'0': {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "OUSD(C) ",
            "VENDOR": "Skillsoft",
            "AUDIENCE": "C-Civilian",
            "COURSEID": "IFS0067",
            "ITEMTYPE": "SEMINAR",
            "COURSENAME": "AFOTEC 301 TMS",
            "Unnamed: 19": "Linked to a JKO-hosted course",
            "508COMPLIANT": "y",
            "SOURCESYSTEM": "DAU",
            "SOURCE_FILES": " N",
            "FLASHIMPACTED": "Y",
            "DELIVERYMETHOD": "",
            "CONVERTEDREMAKE": "N",
            "COURSEDESCRIPTION": "course covers Identify Stakeholders ",
            "MANDATORYTRAINING": "N",
            "SUPERVISORMANAGERIAL": "Y",
            "COMMONMILITARYTRAINING": ""
        }}
        extract_metadata_using_key(entry_1)
        entry_2 = {'0': {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps _ TEST",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "OUSD(C) ",
            "VENDOR": "Skillsoft",
            "AUDIENCE": "C-Civilian",
            "COURSEID": "IFS0067",
            "ITEMTYPE": "SEMINAR",
            "COURSENAME": "AFOTEC 301 TMS",
            "Unnamed: 19": "Linked to a JKO-hosted course",
            "508COMPLIANT": "y",
            "SOURCESYSTEM": "DAU",
            "SOURCE_FILES": " N",
            "FLASHIMPACTED": "Y",
            "DELIVERYMETHOD": "",
            "CONVERTEDREMAKE": "N",
            "COURSEDESCRIPTION": "course covers Identify Stakeholders ",
            "MANDATORYTRAINING": "N",
            "SUPERVISORMANAGERIAL": "Y",
            "COMMONMILITARYTRAINING": ""
        }}
        extract_metadata_using_key(entry_2)
        result_data_2 = MetadataLedger.objects.values(
            'record_lifecycle_status').filter(
            source_metadata_key='DAU_IFS0067')
        self.assertNotEqual(result_data_2[0], result_data_2[1])

    def test_get_source_validation_schema(self):
        """Test to retrieve source validation schema from XIA configuration """
        xiaConfig = XIAConfiguration(source_metadata_schema=
                                     'DAU_Source_Validate_Schema.json')
        xiaConfig.save()
        result_dict = get_source_validation_schema()
        expected_dict = read_json_data('DAU_Source_Validate_Schema.json')
        self.assertEqual(expected_dict, result_dict)

    def test_validate_source_using_key(self):
        """Test for Validating source data against required column names"""
        expected_data = {
            "metadata_record_inactivation_date": "",
            "record_lifecycle_status": "Active",
            "source_metadata": {
                "LMS": "Success Factors LMS v. 5953",
                "OPR": "Marine Corps",
                "XAPI": "Y",
                "SCORM": "N",
                "AGENCY": "OUSD(C) ",
                "VENDOR": "Skillsoft",
                "AUDIENCE": "C-Civilian",
                "COURSEID": "IFS0067",
                "ITEMTYPE": "SEMINAR",
                "COURSENAME": "AFOTEC 301 TMS",
                "Unnamed: 19": "Linked to a JKO-hosted course",
                "508COMPLIANT": "y",
                "SOURCE_FILES": " N",
                "FLASHIMPACTED": "Y",
                "DELIVERYMETHOD": "",
                "CONVERTEDREMAKE": "N",
                "COURSEDESCRIPTION": "course covers the Identify Stakeholders",
                "MANDATORYTRAINING": "N",
                "SUPERVISORMANAGERIAL": "Y",
                "COMMONMILITARYTRAINING": ""
            },
            "source_metadata_hash": "dd83f85b503e052a3997ac945ccdfe02",
            "source_metadata_validation_status": "Y"
        }

        source_data = {
            "source_metadata": {
                "LMS": "Success Factors LMS v. 5953",
                "OPR": "Marine Corps",
                "XAPI": "Y",
                "SCORM": "N",
                "AGENCY": "OUSD(C) ",
                "VENDOR": "Skillsoft",
                "AUDIENCE": "C-Civilian",
                "COURSEID": "IFS0067",
                "ITEMTYPE": "SEMINAR",
                "COURSENAME": "AFOTEC 301 TMS",
                "Unnamed: 19": "Linked to a JKO-hosted course",
                "508COMPLIANT": "y",
                "SOURCE_FILES": " N",
                "FLASHIMPACTED": "Y",
                "DELIVERYMETHOD": "",
                "CONVERTEDREMAKE": "N",
                "COURSEDESCRIPTION": "course covers the Identify Stakeholders",
                "MANDATORYTRAINING": "N",
                "SUPERVISORMANAGERIAL": "Y",
                "COMMONMILITARYTRAINING": ""
            }}
        metadata_ledger = MetadataLedger(record_lifecycle_status='active',
                                         source_metadata=source_data,
                                         source_metadata_hash=
                                         'dd83f85b503e052a3997ac945ccdfe02',
                                         source_metadata_validation_status='',
                                         source_metadata_key='DAU_IFS0067')
        metadata_ledger.save()

        test_data = MetadataLedger.objects.values(
            'source_metadata').filter(source_metadata_validation_status='',
                                      record_lifecycle_status='Active'
                                      ).exclude(
            source_metadata_extraction_date=None)
        required_list = ['SOURCESYSTEM', 'AGENCY', 'COURSEID']
        validate_source_using_key(test_data, required_list)
        result_data = MetadataLedger.objects.values('source_metadata',
                                                    'source_metadata_validation_status').filter(
            source_metadata_key='DAU_IFS0067').first()
        self.assertEqual(expected_data['source_metadata_validation_status'],

                         result_data['source_metadata_validation_status'])

    def test_get_target_validation_schema(self):
        """Test to Retrieve target validation schema from XIA configuration """
        xiaConfig = XIAConfiguration(target_metadata_schema=
                                     'p2881_target_validation_schema.json')
        xiaConfig.save()
        result_target_metadata_schema = get_target_validation_schema()
        self.assertEqual('p2881_target_validation_schema.json',
                         result_target_metadata_schema)
