from uuid import UUID

from django.test import TestCase
from core.models import XIAConfiguration, MetadataLedger
from django.test import tag
from core.management.commands.extract_source_metadata import \
    get_publisher_detail, extract_metadata_using_key
from core.management.commands.validate_source_metadata import \
    get_source_validation_schema, validate_source_using_key
import logging
from core.management.commands.transform_source_metadata import \
    get_target_metadata_for_transformation, transform_source_using_key
from core.management.utils.xsr_client import read_json_data
from core.management.commands.validate_target_metadata import \
    get_target_validation_schema, validate_target_using_key
from core.management.commands.load_target_metadata import \
    renaming_xia_for_posting_to_xis

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
            source_metadata_key='DAU_IFS0067').order_by(
            'record_lifecycle_status')
        self.assertEqual('Active',
                         result_data_2[0]['record_lifecycle_status'])
        self.assertEqual('Inactive',
                         result_data_2[1]['record_lifecycle_status'])
        self.assertNotEqual(result_data_2[0], result_data_2[1])

    def test_get_source_validation_schema(self):
        """Test to retrieve source validation schema from XIA configuration """
        xiaConfig = XIAConfiguration(source_metadata_schema=
                                     'DAU_Source_Validate_Schema.json')
        xiaConfig.save()
        result_dict = get_source_validation_schema()
        expected_dict = read_json_data('DAU_Source_Validate_Schema.json')
        self.assertEqual(expected_dict, result_dict)

    def test_validate_source_using_key_required_column_present(self):
        """Test for Validating source data when the required columns are
        present"""
        source_data = {
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
            "COMMONMILITARYTRAINING": "",
            "SOURCESYSTEM": "DAU"
        }

        metadata_ledger = MetadataLedger(record_lifecycle_status='Active',
                                         source_metadata=source_data,
                                         source_metadata_hash=
                                         'dd83f85b503e052a3997ac945ccdfe02',
                                         source_metadata_key_hash=
                                         'e7d358d47cd56d97dac93e650cfc415b',
                                         source_metadata_key='DAU_IFS0067',
                                         source_metadata_extraction_date=
                                         '2021-02-04 01:26:56.528476')
        metadata_ledger.save()

        test_data = MetadataLedger.objects.values(
            'source_metadata').filter(source_metadata_validation_status='',
                                      record_lifecycle_status='Active'
                                      ).exclude(
            source_metadata_extraction_date=None)
        required_list = ['SOURCESYSTEM', 'AGENCY', 'COURSEID']
        validate_source_using_key(test_data, required_list)

        result_query = MetadataLedger.objects.values(
            'source_metadata_validation_status', ).filter(
            source_metadata_key_hash='e7d358d47cd56d97dac93e650cfc415b'
                                                                    ).first()
        self.assertEqual('Y', result_query.get(
            'source_metadata_validation_status'))

    def test_validate_source_using_key_required_column_missing(self):
        """Test for Validating source data when the required columns are
        missing"""
        source_data = {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "",
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
            "COMMONMILITARYTRAINING": "",
            "SOURCESYSTEM": "DAU"
        }

        metadata_ledger = MetadataLedger(record_lifecycle_status='Active',
                                         source_metadata=source_data,
                                         source_metadata_hash=
                                         'dd83f85b503e052a3997ac945ccdfe02',
                                         source_metadata_key_hash=
                                         'e7d358d47cd56d97dac93e650cfc415b',
                                         source_metadata_key='DAU_IFS0067',
                                         source_metadata_extraction_date=
                                         '2021-02-04 01:26:56.528476')
        metadata_ledger.save()

        test_data = MetadataLedger.objects.values(
            'source_metadata').filter(source_metadata_validation_status='',
                                      record_lifecycle_status='Active'
                                      ).exclude(
            source_metadata_extraction_date=None)
        required_list = ['SOURCESYSTEM', 'AGENCY', 'COURSEID']
        validate_source_using_key(test_data, required_list)

        result_query = MetadataLedger.objects.values(
            'source_metadata_validation_status', ).filter(
            source_metadata_key_hash='e7d358d47cd56d97dac93e650cfc415b'
                                                            ).first()
        self.assertEqual('N', result_query.get(
            'source_metadata_validation_status'))

    def test_get_target_metadata_for_transformation(self):
        """Test that get target mapping_dictionary from XIAConfiguration """

        xiaConfig = XIAConfiguration(
            source_target_mapping='p2881_target_metadata_schema.json')
        xiaConfig.save()
        result_target_mapping_dict = get_target_metadata_for_transformation()
        expected_target_mapping_dict = read_json_data(
            'p2881_target_metadata_schema.json')
        self.assertEqual(result_target_mapping_dict,
                         expected_target_mapping_dict)

    def test_transform_source_using_key(self):
        """Test to transform source metadata to target metadata schema
        format"""

        source_data = {
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
            "COURSEDESCRIPTION": "course covers the Identify Stakeholders",
            "MANDATORYTRAINING": "N",
            "SUPERVISORMANAGERIAL": "Y",
            "COMMONMILITARYTRAINING": ""
        }
        metadata_ledger = MetadataLedger(record_lifecycle_status='active',
                                         source_metadata=source_data,
                                         source_metadata_hash=
                                         'dd83f85b503e052a3997ac945ccdfe02',
                                         source_metadata_validation_status='Y',
                                         source_metadata_key='DAU_IFS0067',
                                         source_metadata_validation_date=
                                         '2021-03-09 03:14:10.698914')
        metadata_ledger.save()

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

        test_data_dict = MetadataLedger.objects.values(
            'source_metadata').filter(
            source_metadata_validation_status='Y',
            record_lifecycle_status='Active').exclude(
            source_metadata_validation_date=None)

        transform_source_using_key(test_data_dict, target_mapping_dict)
        result_data = MetadataLedger.objects.values('target_metadata').filter(
            source_metadata_key='DAU_IFS0067').first()

        self.assertEqual(
            result_data['target_metadata']['Course'].get('CourseCode'),
            source_data['COURSEID'])
        self.assertEqual(
            result_data['target_metadata']['Lifecycle'].get('Provider'),
            source_data['VENDOR'])

    def test_get_target_validation_schema(self):
        """Test to Retrieve target validation schema from XIA configuration """
        xiaConfig = XIAConfiguration(target_metadata_schema=
                                     'p2881_target_validation_schema.json')
        xiaConfig.save()
        result_target_metadata_schema = get_target_validation_schema()
        self.assertEqual('p2881_target_validation_schema.json',
                         result_target_metadata_schema)

    def test_validate_target_using_key_present(self):
        """Test for Validating target data when the required columns are
        present"""
        source_data = {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "",
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
            "COMMONMILITARYTRAINING": "",
            "SOURCESYSTEM": "DAU"
        }

        target_data = {
            "Course": {
                "CourseCode": "oper_24_a02_bs_enus",
                "CourseType": "eLearning",
                "CourseTitle": "Linux Kernel Compilation and Linux Startup",
                "CourseAudience": "Mil/Civ/Contr/Other",
                "DepartmentName": "USSOUTHCOM",
                "CourseDescription": "Interview questions",
                "CourseProviderName": "DAU",
                "EducationalContext": "Mandatory",
                "CourseSectionDeliveryMode": "Internal"
            },
            "Lifecycle": {
                "Provider": "Defense Language Institute",
                "OtherRole": "Success Factors LMS v. 7362",
                "Maintainer": "Ross Major Thomas M"
            }
        }

        metadata_ledger = MetadataLedger(record_lifecycle_status='Active',
                                         source_metadata=source_data,
                                         target_metadata=target_data,
                                         target_metadata_hash=
                                         '205b2df155a2dd4783087af1ad07bca8',
                                         target_metadata_key_hash=
                                         '52c6a7eacac672e03e6a8c60c5fa39c2',
                                         target_metadata_key=
                                         'DAU_oper_24_a02_bs_enus',
                                         source_metadata_transformation_date=
                                         '2021-02-04 01:26:56.528476')
        metadata_ledger.save()

        test_data = MetadataLedger.objects.values(
            'target_metadata').filter(target_metadata_validation_status='',
                                      record_lifecycle_status='Active'
                                      ).exclude(
            source_metadata_transformation_date=None)
        required_dict = {'Course': ['CourseProviderName', 'DepartmentName',
                                    'CourseCode', 'CourseTitle',
                                    'CourseDescription',
                                    'CourseAudience'],
                         'Lifecycle': ['Provider', 'Maintainer']}
        recommended_dict = {'Course': ['EducationalContext'], 'Lifecycle': []}

        validate_target_using_key(test_data, required_dict, recommended_dict)

        result_query = MetadataLedger.objects.values(
            'target_metadata_validation_status', ).filter(
            target_metadata_key_hash='52c6a7eacac672e03e6a8c60c5fa39c2'
                                                                    ).first()

        self.assertEqual('Y', result_query.get(
            'target_metadata_validation_status'))

    def test_validate_target_using_key_missing(self):
        """Test for Validating target data when the required columns are
        missing"""
        source_data = {
            "LMS": "Success Factors LMS v. 5953",
            "OPR": "Marine Corps",
            "XAPI": "Y",
            "SCORM": "N",
            "AGENCY": "",
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
            "COMMONMILITARYTRAINING": "",
            "SOURCESYSTEM": "DAU"
        }

        target_data = {
            "Course": {
                "CourseCode": "oper_24_a02_bs_enus",
                "CourseType": "eLearning",
                "CourseTitle": "Linux Kernel Compilation and Linux Startup",
                "CourseAudience": "Mil/Civ/Contr/Other",
                "DepartmentName": "",
                "CourseDescription": "Interview questions",
                "CourseProviderName": "DAU",
                "EducationalContext": "Mandatory",
                "CourseSectionDeliveryMode": "Internal"
            },
            "Lifecycle": {
                "Provider": " ",
                "OtherRole": "Success Factors LMS v. 7362",
                "Maintainer": "Ross Major Thomas M"
            }
        }

        metadata_ledger = MetadataLedger(record_lifecycle_status='Active',
                                         source_metadata=source_data,
                                         target_metadata=target_data,
                                         target_metadata_hash=
                                         '205b2df155a2dd4783087af1ad07bca8',
                                         target_metadata_key_hash=
                                         '52c6a7eacac672e03e6a8c60c5fa39c2',
                                         target_metadata_key=
                                         'DAU_oper_24_a02_bs_enus',
                                         source_metadata_transformation_date=
                                         '2021-02-04 01:26:56.528476')
        metadata_ledger.save()

        test_data = MetadataLedger.objects.values(
            'target_metadata').filter(target_metadata_validation_status='',
                                      record_lifecycle_status='Active'
                                      ).exclude(
            source_metadata_transformation_date=None)
        required_dict = {'Course': ['CourseProviderName', 'DepartmentName',
                                    'CourseCode', 'CourseTitle',
                                    'CourseDescription',
                                    'CourseAudience'],
                         'Lifecycle': ['Provider', 'Maintainer']}
        recommended_dict = {'Course': ['EducationalContext'], 'Lifecycle': []}
        validate_target_using_key(test_data, required_dict, recommended_dict)

        result_query = MetadataLedger.objects.values(
            'target_metadata_validation_status', ).filter(
            target_metadata_key_hash='52c6a7eacac672e03e6a8c60c5fa39c2'
                                                ).first()

        self.assertEqual('N', result_query.get(
            'target_metadata_validation_status'))

    def test_renaming_xia_for_posting_to_xis(self):
        """Test for Renaming XIA column names to match with XIS column names"""
        xiaConfig = XIAConfiguration(publisher='DAU')
        xiaConfig.save()
        data = {
            'metadata_record_uuid': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'target_metadata': {
                'Course': {
                    'CourseCode': 'DHA-US551',
                    'CourseType': 'STUDY PERIOD',
                    'CourseTitle': 'FOF120 - Pet Quarantine Claim',
                    'CourseAudience': 'PFPA employees only',
                    'DepartmentName': 'DSS/CDSE',
                    'CourseDescription': 'This course explores SharePoint',
                    'CourseProviderName': 'DAU',
                    'EducationalContext': 'Mandatory',
                    'CourseSectionDeliveryMode': 'CMI'
                },
                'Lifecycle': {
                    'Provider': 'Defense Language Institute (DLI)',
                    'OtherRole': 'Success Factors LMS v. 4249',
                    'Maintainer': 'Tommy Bibb MARDET Fort Leonard Wood'
                }
            },
            'target_metadata_hash': '2451c90d70c4df137ee11ed803a8724c',
            'target_metadata_key': 'DAU_DHA-US551',
            'target_metadata_key_hash': '0360d0eb3324ae934c4cc3a58b96c0ba'
        }

        expected_data = {
            'unique_record_identifier': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'metadata': {
                'Course': {
                    'CourseCode': 'DHA-US551',
                    'CourseType': 'STUDY PERIOD',
                    'CourseTitle': 'FOF120 - Pet Quarantine Claim',
                    'CourseAudience': 'PFPA employees only',
                    'DepartmentName': 'DSS/CDSE',
                    'CourseDescription': 'This course explores SharePoint',
                    'CourseProviderName': 'DAU',
                    'EducationalContext': 'Mandatory',
                    'CourseSectionDeliveryMode': 'CMI'
                },
                'Lifecycle': {
                    'Provider': 'Defense Language Institute (DLI)',
                    'OtherRole': 'Success Factors LMS v. 4249',
                    'Maintainer': 'Tommy Bibb MARDET Fort Leonard Wood'
                }
            },
            'metadata_hash': '2451c90d70c4df137ee11ed803a8724c',
            'metadata_key': 'DAU_DHA-US551',
            'metadata_key_hash': '0360d0eb3324ae934c4cc3a58b96c0ba',
            'provider_name': 'DAU'
        }
        return_data = renaming_xia_for_posting_to_xis(data)
        self.assertEquals(expected_data['metadata_hash'],
                          return_data['metadata_hash'])
        self.assertEquals(expected_data['metadata_key'],
                          return_data['metadata_key'])
        self.assertEquals(expected_data['metadata_key_hash'],
                          return_data['metadata_key_hash'])
        self.assertEquals(expected_data['provider_name'],
                          return_data['provider_name'])
