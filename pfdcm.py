import requests
from loguru import logger
import sys
import copy
from collections import ChainMap
import json

LOG = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)

def health_check(url: str):
    pfdcm_about_api = f'{url}about/'
    headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
    try:
        response = requests.get(pfdcm_about_api, headers=headers)
        return response
    except Exception as er:
        raise Exception("Connection to pfdcm could not be established.")


def retrieve_pacsfiles(directive: dict, url: str, pacs_name: str):
    """
    This method uses the async API endpoint of `pfdcm` to send a single 'retrieve' request that in
    turn uses `oxidicom` to push and register PACS files to a CUBE instance
    """

    pfdcm_dicom_api = f'{url}PACS/thread/pypx/'
    headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
    body = {
        "PACSservice": {
            "value": pacs_name
        },
        "listenerService": {
            "value": "default"
        },
        "PACSdirective": {
            "withFeedBack": True,
            "then": "retrieve",
            "thenArgs": '',
            "dblogbasepath": '/home/dicom/log',
            "json_response": False
        }
    }
    body["PACSdirective"].update(directive)
    LOG(f"request : {body}")

    try:
        response = requests.post(pfdcm_dicom_api, json=body, headers=headers)
        d_response = json.loads(response.text)
        if d_response['response']['job']['status']:
            return d_response
        else:
            raise Exception(d_response['message'])
    except Exception as er:
        LOG(er)