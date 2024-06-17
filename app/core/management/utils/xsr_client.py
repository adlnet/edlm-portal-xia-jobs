import hashlib
import json
import logging
from datetime import datetime

import html2text
import pandas as pd
import requests
from bs4 import BeautifulSoup
from openlxp_xia.management.utils.xia_internal import (
    dict_flatten, get_key_dict, traverse_dict_with_key_list)

from core.models import XSRConfiguration

logger = logging.getLogger('dict_config_logger')


def get_xsr_api_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    logger.debug("Retrieve xsr_api_endpoint from XSR configuration")
    xsr_obj = XSRConfiguration.objects.first()

    return xsr_obj.xsr_api_endpoint, xsr_obj.token


def get_xsr_api_response():
    """Function to get api response from xsr endpoint"""
    # url of rss feed
    xsr_data, token = get_xsr_api_endpoint()

    url = xsr_data + ("/webservice/rest/server."
                      "php?wstoken=")+token+(
                      "&wsfunction="
                      "core_course_get_courses_by_"
                      "field&moodlewsrestformat=json")

    # creating HTTP response object from given url
    try:
        resp = requests.get(url, verify=False)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        raise SystemExit('Exiting! Can not make connection with XSR.')

    return resp


# Function to convert
def listToString(s):
    # initialize an empty string
    str1 = ", "
    # return string
    return (str1.join(s))


def custom_moodle_fields(source_data_dict):
    """Function to format data specific to moodle"""

    xsr_api_end, token = get_xsr_api_endpoint()
    source_ecc_approved_dict = []
    # filter out only ECC approved courses
    for source_course in source_data_dict:
        try:
            if "ecc approved" in source_course['categoryname'].lower():
                source_course['courseurl'] = (xsr_api_end +
                                              "/enrol/index.php?id="
                                              + str(source_course['id']))
                source_ecc_approved_dict.append(source_course)
        except ValueError:
            logger.error("Source data requires a category name "
                         "field for filtering ECC approved data")

    for source_course in source_ecc_approved_dict:
        contacts = []
        try:
            # getting length of list
            length_custom_fields = len(source_course['customfields'])
            # Iterating the index
            # Iterating through custom fields
            for i in range(length_custom_fields):
                key = source_course['customfields'][i]['shortname']
                value = source_course['customfields'][i]['value']
                # setting new custom field key and value pairs to source
                source_course[key] = value
        except ValueError:
            logger.warning("Source data does not contain custom fields")

        try:
            # getting length of list
            length_contacts = len(source_course['contacts'])
            # Iterating the index
            # Iterating through contacts
            for i in range(length_contacts):
                contacts.append(source_course['contacts'][i]['fullname'])
            contact_str = listToString(contacts)
            source_course['instructor'] = contact_str
        except ValueError:
            logger.warning("Source data does not contain contact fields")

        try:
            # setting list as string value
            enrollment_str = listToString(source_course['enrollmentmethods'])
            source_course['enrollmentmethods'] = enrollment_str
        except ValueError:
            logger.warning("Source data does not "
                           "contain enrollment methods fields")

    return source_ecc_approved_dict


def extract_source():
    """function to parse xml xsr data and convert to dictionary"""

    resp = get_xsr_api_response()
    source_data_dict = json.loads(resp.text)
    source_data_dict = source_data_dict["courses"]

    # format data specific to moodle
    source_ecc_approved_dict = custom_moodle_fields(source_data_dict)

    logger.info("Retrieving data from source page ")
    source_df_list = [pd.DataFrame(source_ecc_approved_dict)]
    source_df_final = pd.concat(source_df_list).reset_index(drop=True)
    logger.info("Completed retrieving data from source")
    return source_df_final


def read_source_file():
    """sending source data in dataframe format"""
    logger.info("Retrieving data from XSR")
    # load rss from web to convert to xml
    xsr_items = extract_source()
    # convert xsr dictionary list to Dataframe
    source_df = pd.DataFrame(xsr_items)
    source_df.to_csv("file_name.csv", sep='\t', encoding='utf-8')
    logger.info("Changing null values to None for source dataframe")
    std_source_df = source_df.where(pd.notnull(source_df),
                                    None)
    return [std_source_df]


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['idnumber', 'SOURCESYSTEM']
    field_values = []

    for item in field:
        if not data_dict.get(item):
            logger.error('Field name ' + item + ' is missing for '
                                                'key creation')
            return None
        field_values.append(data_dict.get(item))

    # Key value creation for source metadata
    key_value = '_'.join(field_values)

    # Key value hash creation for source metadata
    key_value_hash = hashlib.sha512(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key


def convert_int_to_date(element, target_data_dict):
    """Convert integer date to date time"""
    key_list = element.split(".")
    check_key_dict = target_data_dict
    check_key_dict = traverse_dict_with_key_list(check_key_dict, key_list)
    if check_key_dict:
        if key_list[-1] in check_key_dict:
            if isinstance(check_key_dict[key_list[-1]], int):
                check_key_dict[key_list[-1]] = datetime. \
                    fromtimestamp(check_key_dict[key_list[-1]])


def find_dates(data_dict):
    """Function to convert integer value to date value """

    data_flattened = dict_flatten(data_dict, [])

    for element in data_flattened.keys():
        element_lower = element.lower()
        if (element_lower.find("date") != -1 or element_lower.find(
                "time")) != -1:
            convert_int_to_date(element, data_dict)
    return data_dict


def convert_html(element, target_data_dict):
    """Convert HTML to text data"""
    key_list = element.split(".")
    check_key_dict = target_data_dict
    check_key_dict = traverse_dict_with_key_list(check_key_dict, key_list)
    if check_key_dict:
        if key_list[-1] in check_key_dict:
            check_key_dict[key_list[-1]] = \
                html2text.html2text(check_key_dict[key_list[-1]])


def find_html(data_dict):
    """Function to convert HTML value to text"""
    data_flattened = dict_flatten(data_dict, [])

    for element in data_flattened.keys():
        if data_flattened[element]:
            if bool(BeautifulSoup(str(data_flattened[element]),
                                  "html.parser").find()):
                convert_html(element, data_dict)
    return data_dict
