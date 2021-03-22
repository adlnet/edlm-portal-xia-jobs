import os
import logging
import pandas as pd
import requests
import xml.etree.ElementTree as ET

logger = logging.getLogger('dict_config_logger')


def get_xsr_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    xsr_endpoint = os.environ.get('XSR_ENDPOINT')
    return xsr_endpoint


def extract_source():
    """function to parse xml xsr data and convert to dictionary"""

    # url of rss feed
    url = get_xsr_endpoint()

    # creating HTTP response object from given url
    resp = requests.get(url)

    # saving the xml file
    xml_content = resp.text

    # create element tree object
    xsr_root = ET.fromstring(xml_content)

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
