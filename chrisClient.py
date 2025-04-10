### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json
import time
from loguru import logger
import sys
from pipeline import Pipeline

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
        self.cl.pacs_series_url = f"{url}pacs/series/"
        self.req = PACSClient(self.cl.pacs_series_url,username,password)

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
        pipe.workflow_schedule(dsdir_inst_id, "DICOM anonymization, niftii conversion, and push to neuro tree v20250326",
                               plugin_params)

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
        response = self.cl.create_plugin_instance(plugin_id, params)
        return response['id']

    def __get_plugin_id(self, params: dict):
        response = self.cl.get_plugins(params)
        if response['total'] > 0:
            return response['data'][0]['id']
        raise Exception(f"No plugin found with matching search criteria {params}")