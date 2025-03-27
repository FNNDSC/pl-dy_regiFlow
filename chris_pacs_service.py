import requests
from chrisclient import client, request
from loguru import logger
import sys

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


class PACSClient(object):
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
        self.cl.pacs_series_url = f"{url}pacs/series/"
        self.req = request.Request(username, password)

    def get_pacs_registered(self, params: dict):
        """
        Get the list of PACS series registered to _this_
        CUBE instance
        """
        response = self.cl.get_pacs_series_list(params)
        if response:
            return response['total']
        raise Exception(f"No PACS details with matching search criteria {params}")

    def get_pacs_files(self, params: dict):
        """
        Get PACS folder path
        """
        l_dir_path = set()
        resp = self.req.get(f"{self.cl.pacs_series_url}search", params)
        for item in resp.items:
            for link in item.links:
                folder = self.req.get(link.href)
                for item_folder in folder.items:
                    path = item_folder.data.path.value
                    l_dir_path.add(path)
        return ','.join(l_dir_path)