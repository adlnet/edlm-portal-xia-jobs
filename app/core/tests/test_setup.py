from uuid import UUID

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

        self.target_metadata = {
            "Course": {
                "CourseCode": "TestData 123",
                "CourseTitle": "Acquisition Law",
                "CourseAudience": "test_data",
                "DepartmentName": "",
                "CourseObjective": "test_data",
                "CourseDescription": "test description",
                "CourseProviderName": "DAU",
                "CourseSpecialNotes": "test_data",
                "CoursePrerequisites": "None",
                "EstimatedCompletionTime": "4.5 days",
                "CourseSectionDeliveryMode": "Resident",
                "CourseAdditionalInformation": "None"
            },
            "CourseInstance": {
                "CourseURL": "https://dau.tes.com/ui/lms-learning-details"
            },
            "General_Information": {
                "EndDate": "end_date",
                "StartDate": "start_date"
            }
        }

        self.target_key_value = "TestData 123_DAU"
        self.target_key_value_hash = "6acf7689ea81a1f792e7668a23b1acf5"
        self.target_hash_value = "df0b51d7b45ca29682e930d236963584"
        self.schema_data_dict = {
            'SOURCESYSTEM': 'Required',
            'test_id': 'Optional',
            'crs_header': 'Required',
            'test_name': 'Required',
            'test_description': 'Required',
            'test_objective': 'Optional',
            'test_attendies': 'Optional',
            'test_images': 'Optional',
            'test1_id': 'Optional',
            'test_url': 'Optional',
            'Start_date': 'Required',
            'End_date': 'Required',
            'Test_current': 'Recommended'
        }

        self.target_data_dict = {
            'Course': {
                'CourseProviderName': 'Required',
                'DepartmentName': 'Optional',
                'CourseCode': 'Required',
                'CourseTitle': 'Required',
                'CourseDescription': 'Required',
                'CourseShortDescription': 'Required',
                'CourseFullDescription': 'Optional',
                'CourseAudience': 'Optional',
                'CourseSectionDeliveryMode': 'Optional',
                'CourseObjective': 'Optional',
                'CoursePrerequisites': 'Optional',
                'EstimatedCompletionTime': 'Optional',
                'CourseSpecialNotes': 'Optional',
                'CourseAdditionalInformation': 'Optional',
                'CourseURL': 'Optional',
                'CourseLevel': 'Optional',
                'CourseSubjectMatter': 'Required'
            },
            'CourseInstance': {
                'CourseCode': 'Required',
                'CourseTitle': 'Required',
                'Thumbnail': 'Recommended',
                'CourseShortDescription': 'Optional',
                'CourseFullDescription': 'Optional',
                'CourseURL': 'Optional',
                'StartDate': 'Required',
                'EndDate': 'Required',
                'EnrollmentStartDate': 'Optional',
                'EnrollmentEndDate': 'Optional',
                'DeliveryMode': 'Required',
                'InLanguage': 'Optional',
                'Instructor': 'Required',
                'Duration': 'Optional',
                'CourseLearningOutcome': 'Optional',
                'CourseLevel': 'Optional',
                'InstructorBio': 'Optional'
            },
            'General_Information': {
                'StartDate': 'Required',
                'EndDate': 'Required'
            },
            'Technical_Information': {
                'Thumbnail': 'Recommended'
            }
        }

        self.xia_data = {
            'metadata_record_uuid': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'target_metadata': {
                "Course": {
                    "CourseCode": "TestData 123",
                    "CourseTitle": "Acquisition Law",
                    "CourseAudience": "test_data",
                    "DepartmentName": "",
                    "CourseObjective": "test_data",
                    "CourseDescription": "test_data",
                    "CourseProviderName": "DAU",
                    "CourseSpecialNotes": "test_data",
                    "CoursePrerequisites": "None",
                    "EstimatedCompletionTime": "4.5 days",
                    "CourseSectionDeliveryMode": "Resident",
                    "CourseAdditionalInformation": "None"
                },
                "CourseInstance": {
                    "CourseURL": "https://dau.tes.com/ui/lms-learning-details"
                },
                "General_Information": {
                    "EndDate": "end_date",
                    "StartDate": "start_date"
                }
            },
            'target_metadata_hash': 'df0b51d7b45ca29682e930d236963584',
            'target_metadata_key': 'TestData 123_DAU',
            'target_metadata_key_hash': '6acf7689ea81a1f792e7668a23b1acf5'
        }

        self.xis_expected_data = {
            'unique_record_identifier': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'metadata': {
                "Course": {
                    "CourseCode": "TestData 123",
                    "CourseTitle": "Acquisition Law",
                    "CourseAudience": "test_data",
                    "DepartmentName": "",
                    "CourseObjective": "test_data",
                    "CourseDescription": "test_data",
                    "CourseProviderName": "DAU",
                    "CourseSpecialNotes": "test_data",
                    "CoursePrerequisites": "None",
                    "EstimatedCompletionTime": "4.5 days",
                    "CourseSectionDeliveryMode": "Resident",
                    "CourseAdditionalInformation": "None"
                },
                "CourseInstance": {
                    "CourseURL": "https://dau.tes.com/ui/lms-learning-details"
                },
                "General_Information": {
                    "EndDate": "end_date",
                    "StartDate": "start_date"
                }
            },
            'metadata_hash': 'df0b51d7b45ca29682e930d236963584',
            'metadata_key': 'TestData 123_DAU',
            'metadata_key_hash': '6acf7689ea81a1f792e7668a23b1acf5',
            'provider_name': 'DAU'
        }

        self.source_target_mapping = {
            "Course": {
                "CourseProviderName": "SOURCESYSTEM",
                "DepartmentName": "",
                "CourseCode": "crs_header",
                "CourseTitle": "test_name",
                "CourseDescription": "test_description",
                "CourseAudience": "test_attendies",
                "CourseSectionDeliveryMode": "test_mode",
                "CourseObjective": "test_objective",
                "CoursePrerequisites": "test_prerequisite",
                "EstimatedCompletionTime": "test_length",
                "CourseSpecialNotes": "test_notes",
                "CourseAdditionalInformation": "test_postscript"
            },
            "CourseInstance": {
                "CourseURL": "test_url"
            },
            "General_Information": {
                "StartDate": "start_date",
                "EndDate": "end_date"
            }
        }
        self.metadata_invalid = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "",
            "Start_date": "",
            "crs_header": "TestData 1234",
            "SOURCESYSTEM": "DAU",
            "test_description": "test description",
        }

        self.key_value_invalid = "TestData 1234_DAU"
        self.key_value_hash_invalid = "7b6e3f29c7e76cef2fba617427fa20b1"
        self.hash_value_invalid = "f15860eed6e88ddfcce71059043f34a3"

        self.target_metadata_invalid = {
            "Course": {
                "CourseCode": "TestData 1234",
                "CourseTitle": "Acquisition Law",
                "CourseAudience": "test_data",
                "DepartmentName": "",
                "CourseObjective": "test_data",
                "CourseDescription": "",
                "CourseProviderName": "DAU",
                "CourseSpecialNotes": "test_data",
                "CoursePrerequisites": "None",
                "EstimatedCompletionTime": "4.5 days",
                "CourseSectionDeliveryMode": "Resident",
                "CourseAdditionalInformation": "None"
            },
            "CourseInstance": {
                "CourseURL": "https://dau.tes.com/ui/lms-learning-details"
            },
            "General_Information": {
                "EndDate": "end_date",
                "StartDate": "start_date"
            }
        }
        self.target_key_value_invalid = "TestData 1234_DAU"
        self.target_key_value_hash_invalid = "7b6e3f29c7e76cef2fba617427fa20b1"
        self.target_hash_value_invalid = "f15860eed6e88ddfcce71059043f34a3"

        self.test_required_column_names = ['SOURCESYSTEM', 'crs_header',
                                           'Start_date', 'End_date']
        return super().setUp()

    def tearDown(self):
        return super().tearDown()
