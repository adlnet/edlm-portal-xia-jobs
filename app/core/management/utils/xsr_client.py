import hashlib
import logging
import xml.etree.ElementTree as element_Tree

import pandas as pd
import requests
from openlxp_xia.management.utils.xia_internal import get_key_dict

from core.models import XSRConfiguration

logger = logging.getLogger('dict_config_logger')


def get_xsr_api_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    logger.debug("Retrieve xsr_api_endpoint from XSR configuration")
    xsr_data = XSRConfiguration.objects.first()
    xsr_api_endpoint = xsr_data.xsr_api_endpoint
    return xsr_api_endpoint


def get_xsr_api_response():
    """Function to get api response from xsr endpoint"""
    # url of rss feed
    url = get_xsr_api_endpoint()

    # creating HTTP response object from given url
    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        raise SystemExit('Exiting! Can not make connection with XSR.')

    return resp


def extract_source():
    """function to parse xml xsr data and convert to dictionary"""

    resp = get_xsr_api_response()

    # saving the xml file
    xml_content = resp.text

    # create element tree object
    xsr_root = element_Tree.fromstring(xml_content)

    xsr_items = []
    for item in xsr_root.findall('.//Table'):
        # empty news dictionary
        xsr_dict = {}
        # iterate child elements of item
        for child in item:
            xsr_dict[child.tag] = child.text
        # append xsr dictionary to xsr items list
        xsr_items.append(xsr_dict)
    return xsr_items


def read_source_file():
    """sending source data in dataframe format"""
    logger.info("Retrieving data from XSR")
    # load rss from web to convert to xml
    xsr_items = extract_source()
    # convert xsr dictionary list to Dataframe
    source_df = pd.DataFrame(xsr_items)
    logger.info("Changing null values to None for source dataframe")
    std_source_df = source_df.where(pd.notnull(source_df),
                                    None)
    return [std_source_df]


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['crs_header', 'SOURCESYSTEM']
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
    key_value_hash = hashlib.md5(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key
