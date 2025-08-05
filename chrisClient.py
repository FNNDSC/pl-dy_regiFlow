### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json
import time
from loguru import logger
import sys
from pipeline import Pipeline
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from urllib.parse import urlencode

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

class ChrisClient(BaseClient):
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
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
        url = f"{self.pacs_series_url}{endpoint}"
        response = requests.request(method, endpoint, headers=self.headers, auth=self.auth, timeout=5, **kwargs)
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text

    def create_con(self,params:dict):
        return self.cl

    def health_check(self):
        return self.cl.get_chris_instance()

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass

    def anonymize(self, dicom_dir: str, send_params: dict, pv_id: int):
        dsdir_inst_id = self.pl_run_dicomdir(dicom_dir,pv_id)
        plugin_params = {
            'send-dicoms-to-neuro-FS': {
                "path": f"{send_params["neuro_dcm_location"]}/{send_params["folder_name"]}/",
                "include": "*.dcm",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            'send-anon-dicoms-to-neuro-FS': {
                "path": f"{send_params["neuro_anon_location"]}/{send_params["folder_name"]}/",
                "include": "*.dcm",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            'send-niftii-to-neuro-FS': {
                "path": f"{send_params["neuro_nifti_location"]}/{send_params["folder_name"]}/",
                "include": "*",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            }
        }
        pipe = Pipeline(self.cl)
        pipe.run_pipeline(previous_inst=dsdir_inst_id,
                          pipeline_name ="DICOM anonymization, niftii conversion, and push to neuro tree v20250326",
                          pipeline_params=plugin_params)

    def pl_run_dicomdir(self, dicom_dir: str, pv_id: int) -> int:
        pl_id = self.__get_plugin_id({"name": "pl-dsdircopy", "version": "1.0.2"})
        # 1) Run dircopy
        # empty directory check
        if len(dicom_dir) == 0:
            LOG(f"No directory found in CUBE containing files for search")
            return
        pv_in_id = self.__create_feed(pl_id, {"previous_id": pv_id, 'dir': dicom_dir})
        return int(pv_in_id)

    def __create_feed(self, plugin_id: str,params: dict):
        response = self.make_request("POST",f"{self.api_base}/plugins/{plugin_id}/instances/",json=params)
        if response:
            for item in response.get("collection", {}).get("items", []):
                for field in item.get("data", []):
                    if field.get("name") == "id":
                        return field.get("value")
        raise Exception(f"Plugin instance could not be scheduled")

    def __get_plugin_id(self, params: dict):
        query_string = urlencode(params)
        response = self.make_request("GET", f"{self.api_base}/plugins/search/?{query_string}")
        if response:
            for item in response.get("collection", {}).get("items", []):
                for field in item.get("data", []):
                    if field.get("name") == "id":
                        return field.get("value")
        raise Exception(f"No plugin found with matching search criteria {params}")