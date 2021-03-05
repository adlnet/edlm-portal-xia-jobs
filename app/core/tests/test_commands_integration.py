from django.test import TestCase
from core.models import XIAConfiguration, MetadataLedger
from django.test import tag
import pandas as pd
from core.management.commands.extract_source_metadata import \
    get_publisher_detail, extract_metadata_using_key
import logging

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
        logger.info(expected_data['source_metadata'].get('LMS'))

        self.assertEqual(expected_data['source_metadata'].get('LMS'), a['LMS'])
        self.assertEqual(expected_data['source_metadata'].get('OPR'), a['OPR'])
        self.assertEqual(expected_data['source_metadata'].get('FLASHIMPACTED'),
                         a['FLASHIMPACTED'])





