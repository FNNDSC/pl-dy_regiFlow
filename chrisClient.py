### Chris Client Implementation ###

import sys
import json
import requests
from loguru import logger
from urllib.parse import urlencode
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from base_client import BaseClient
from pipeline import Pipeline

# ----------------------------------------
# Logger Configuration
# ----------------------------------------
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
LOG = logger.debug


class ChrisClient(BaseClient):
    def __init__(self, url: str, token: str):
        self.api_base = url.rstrip('/')
        self.token = token
        self.headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}

    # ----------------------------------------
    # Retryable request handler
    # ----------------------------------------
    @retry(
        retry=retry_if_exception_type((RequestException, Timeout, HTTPError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        reraise=True
    )
    def make_request(self, method: str, endpoint: str, **kwargs):
        response = requests.request(
            method, endpoint, headers=self.headers, timeout=30, **kwargs
        )
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text

    def post_request(self, endpoint: str, **kwargs):
        response = requests.request(
            "POST", endpoint, headers=self.headers, timeout=30, **kwargs
        )
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text


    def create_con(self, params: dict):
        pass

    def health_check(self):
        endpoint = f"{self.api_base}/"
        response = requests.request("GET", endpoint, headers=self.headers, timeout=30)

        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text

    def pacs_pull(self):
        pass  # Placeholder for PACS pull implementation

    def pacs_push(self):
        pass  # Placeholder for PACS push implementation

    async def anonymize(self, dicom_dir: str, send_params: dict, pv_id: int, series_data: str):
        """
        Run the anonymization pipeline for a given DICOM directory and push results to specified neuro locations.
        """
        d_series = json.loads(series_data)
        d_series['Folder Name'] = send_params['folder_name']
        d_series['SeriesDescription'] = dicom_dir.split('/')[-1]
        dsdir_inst_id = self.run_dicomdir_plugin(dicom_dir, pv_id)

        plugin_params = {
            'send-dicoms-to-neuro-FS': {
                "path": f"{send_params['neuro_dcm_location']}/{send_params['folder_name']}/",
                "include": "*.dcm",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            'send-anon-dicoms-to-neuro-FS': {
                "path": f"{send_params['neuro_anon_location']}/{send_params['folder_name']}/",
                "include": "*.dcm",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            },
            'send-niftii-to-neuro-FS': {
                "path": f"{send_params['neuro_nifti_location']}/{send_params['folder_name']}/",
                "include": "*",
                "min_size": "0",
                "timeout": "0",
                "max_size": "1G",
                "max_depth": "3"
            }
        }

        pipe = Pipeline(self.api_base, self.token)
        d_ret = await pipe.run_pipeline(
            previous_inst=dsdir_inst_id,
            pipeline_name="DICOM anonymization, niftii conversion, and push to neuro tree v20250326",
            pipeline_params=plugin_params,
            recipients=send_params['recipients'],
            smtp_server=send_params['smtp_server'],
            series_data=json.dumps(d_series)
        )
        return d_ret

    def run_dicomdir_plugin(self, dicom_dir: str, pv_id: int) -> int:
        """
        Run the pl-dsdircopy plugin on a DICOM directory.
        """
        try:
            if not dicom_dir:
                LOG("No directory found in CUBE containing files for search.")
                raise ValueError("Empty DICOM directory path provided.")

            plugin_id = self._get_plugin_id({"name": "pl-dsdircopy", "version": "1.0.2"})
            instance_id = self._create_plugin_instance(plugin_id, {
                "previous_id": pv_id,
                "dir": dicom_dir
            })
            return int(instance_id)
        except Exception as ex:
            LOG(f"Error occurred while creating dsdircopy instance {ex}")

    def _create_plugin_instance(self, plugin_id: str, params: dict):
        """
        Create a plugin instance and return its ID.
        """
        response = self.post_request( f"{self.api_base}/plugins/{plugin_id}/instances/", json=params)
        items = response.get("collection", {}).get("items", [])

        for item in items:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")

        raise RuntimeError("Plugin instance could not be scheduled.")

    def _get_plugin_id(self, params: dict):
        """
        Fetch plugin ID by search parameters.
        """
        query_string = urlencode(params)
        response = self.make_request("GET", f"{self.api_base}/plugins/search/?{query_string}")
        items = response.get("collection", {}).get("items", [])

        for item in items:
            for field in item.get("data", []):
                if field.get("name") == "id":
                    return field.get("value")

        raise RuntimeError(f"No plugin found with matching criteria: {params}")
