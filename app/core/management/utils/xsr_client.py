import logging
import os
import xml.etree.ElementTree as element_Tree

import pandas as pd
import requests

logger = logging.getLogger('dict_config_logger')


def get_xsr_api_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    xsr_api_endpoint = os.environ.get('XSR_API_ENDPOINT')
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
    return std_source_df
