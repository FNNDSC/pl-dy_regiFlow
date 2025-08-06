import requests
from chrisclient import client, request
from loguru import logger
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from urllib.parse import urlencode
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
        self.cl = client
        self.api_base = url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.headers = {"Content-Type": "application/json"}
        self.pacs_series_url = f"{self.api_base}/pacs/series"

    # --------------------------
    # Retryable request handler
    # --------------------------
    @retry(
        retry=retry_if_exception_type((RequestException, Timeout, HTTPError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        reraise=True
    )
    def make_request(self, method, endpoint, **kwargs):
        response = requests.request(method, endpoint, headers=self.headers, auth=self.auth, timeout=5, **kwargs)
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text

    def get_pacs_registered(self, params: dict):
        """
        Get the list of PACS series registered to _this_
        CUBE instance
        """
        query_string = urlencode(params)
        response = self.make_request("GET",f"{self.pacs_series_url}/search/?{query_string}")
        if response:
            return response.get("collection", {}).get("total", [])
        raise Exception(f"No PACS details with matching search criteria {params}")

    def get_pacs_files(self, params: dict):
        """
        Get PACS folder path
        """
        l_dir_path = set()
        query_string = urlencode(params)
        response = self.make_request("GET", f"{self.pacs_series_url}/search/?{query_string}")
        for item in response.get("collection", {}).get("items", []):
            for link in item.get("links", []):
                folder = self.make_request("GET",link.get("href"))
                for item_folder in folder.get("collection", {}).get("items", []):
                    for field in item_folder.get("data", []):
                        if field.get("name") == "path":
                            l_dir_path.add(field.get("value"))

        return ','.join(l_dir_path)