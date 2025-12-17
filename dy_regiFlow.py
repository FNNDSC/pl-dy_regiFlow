#!/usr/bin/env python

from pathlib import Path
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter
from chris_pacs_service import PACSClient
from loguru import logger
from chris_plugin import chris_plugin, PathMapper
from chrisClient import ChrisClient
import time
import json
import copy
import sys
import os
import pfdcm
import copy
import asyncio

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

__version__ = '1.1.2'

DISPLAY_TITLE = r"""
       _           _                          _______ _               
      | |         | |                        (_)  ___| |              
 _ __ | |______ __| |_   _     _ __ ___  __ _ _| |_  | | _____      __
| '_ \| |______/ _` | | | |   | '__/ _ \/ _` | |  _| | |/ _ \ \ /\ / /
| |_) | |     | (_| | |_| |   | | |  __/ (_| | | |   | | (_) \ V  V / 
| .__/|_|      \__,_|\__, |   |_|  \___|\__, |_\_|   |_|\___/ \_/\_/  
| |                   __/ |_____         __/ |                        
|_|                  |___/______|       |___/                         
"""


parser = ArgumentParser(description='A dynamic plugin to check registration and run anonymization pipeline',
                        formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument(
    '--PACSurl',
    default='',
    type=str,
    help='endpoint URL of pfdcm'
)
parser.add_argument(
    '--PACSname',
    default='MINICHRISORTHANC',
    type=str,
    help='name of the PACS'
)
parser.add_argument(
    "--CUBEurl",
    default="http://localhost:8000/api/v1/",
    help="CUBE URL"
)
parser.add_argument(
    "--pluginInstanceID",
    default="",
    help="plugin instance ID from which to start analysis",
)
parser.add_argument(
    "--CUBEtoken",
    default="",
    help="CUBE/ChRIS auth token"
)
parser.add_argument(
    '--inputJSONfile',
    default='',
    type=str,
    help='JSON file containing DICOM data to be retrieved'
)
parser.add_argument(
    '--neuroDicomLocation',
    default='',
    type=str,
    help='a location in the neuro tree for DICOM'
)
parser.add_argument(
    '--neuroAnonLocation',
    default='',
    type=str,
    help='a location in the neuro tree for anon DICOM'
)
parser.add_argument(
    '--neuroNiftiLocation',
    default='',
    type=str,
    help='a location in the neuro tree for nifti'
)
parser.add_argument(
    '--folderName',
    default='',
    type=str,
    help='folder name in neuro tree to push analysis data'
)
parser.add_argument(
    '--pollInterval',
    default=5,
    type=int,
    help='wait time in seconds before the next poll'
)
parser.add_argument(
    '--maxPoll',
    default=50,
    type=int,
    help='max number of times to poll before error out'
)
parser.add_argument(
    "--inNode",
    help="perform in-node implicit parallelization in conjunction with --thread",
    dest="inNode",
    action="store_true",
    default=False,
)
parser.add_argument(
    "--thread",
    help="use threading to branch in parallel",
    dest="thread",
    action="store_true",
    default=False,
)
parser.add_argument(
    '--recipients',
    default='',
    type=str,
    help='comma separated valid email recipient addresses'
)
parser.add_argument(
    '--SMTPServer',
    default='mailsmtp4.childrenshospital.org',
    type=str,
    help='valid email server'
)
parser.add_argument('-V', '--version', action='version',
                    version=f'%(prog)s {__version__}')


# The main function of this *ChRIS* plugin is denoted by this ``@chris_plugin`` "decorator."
# Some metadata about the plugin is specified here. There is more metadata specified in setup.py.
#
# documentation: https://fnndsc.github.io/chris_plugin/chris_plugin.html#chris_plugin
@chris_plugin(
    parser=parser,
    title='A dynamic registration workflow control plugin',
    category='',                 # ref. https://chrisstore.co/plugins
    min_memory_limit='100Mi',    # supported units: Mi, Gi
    min_cpu_limit='1000m',       # millicores, e.g. "1000m" = 1 CPU core
    min_gpu_limit=0              # set min_gpu_limit=1 to enable GPU
)
def main(options: Namespace, inputdir: Path, outputdir: Path):
    """
    *ChRIS* plugins usually have two positional arguments: an **input directory** containing
    input files and an **output directory** where to write output files. Command-line arguments
    are passed to this main method implicitly when ``main()`` is called below without parameters.

    :param options: non-positional arguments parsed by the parser given to @chris_plugin
    :param inputdir: directory containing (read-only) input files
    :param outputdir: directory where to write output files
    """

    print(DISPLAY_TITLE)

    # Typically it's easier to think of programs as operating on individual files
    # rather than directories. The helper functions provided by a ``PathMapper``
    # object make it easy to discover input files and write to output files inside
    # the given paths.
    #
    # Refer to the documentation for more options, examples, and advanced uses e.g.
    # adding a progress bar and parallelism.
    log_file = os.path.join(outputdir, 'terminal.log')
    logger.add(log_file)
    if not health_check(options): return

    cube_cl = PACSClient(options.CUBEurl, options.CUBEtoken)
    mapper = PathMapper.file_mapper(inputdir, outputdir, glob=options.inputJSONfile)
    for input_file, output_file in mapper:

        # Open and read the JSON file
        with open(input_file, 'r') as file:
            data = json.load(file)

            # null check
            if len(data) == 0:
                raise Exception(f"Cannot verify registration for empty pacs data.")

            retry_table = create_hash_table(data, 5)
            registration_errors = asyncio.run(check_registration(options, retry_table, cube_cl))

            if registration_errors:
                LOG(f"ERROR while running pipelines.")
                sys.exit(1)

def sanitize_for_cube(series: dict) -> dict:
    """
    TBD
    """
    params = {"SeriesInstanceUID": series["SeriesInstanceUID"], "StudyInstanceUID": series["StudyInstanceUID"]}
    return params

def health_check(options) -> bool:
    """
    check if connections to pfdcm, orthanc, and CUBE is valid
    """
    try:
        if not options.pluginInstanceID:
            options.pluginInstanceID = os.environ['CHRIS_PREV_PLG_INST_ID']
    except Exception as ex:
        LOG(ex)
        return False
    try:
        # create connection object
        if not options.CUBEtoken:
            options.CUBEtoken = os.environ['CHRIS_USER_TOKEN']
        cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
        cube_con.health_check()
    except Exception as ex:
        LOG(ex)
        return False
    return True

def create_hash_table(retrieve_data: dict, retry: int) -> dict:
    retry_table: dict = {}
    for series in retrieve_data:
        retry_table[series["SeriesInstanceUID"]] = {}
        retry_table[series["SeriesInstanceUID"]]["retry"] = retry
        retry_table[series["SeriesInstanceUID"]]["SeriesInstanceUID"] = series["SeriesInstanceUID"]
        retry_table[series["SeriesInstanceUID"]]["StudyInstanceUID"] = series["StudyInstanceUID"]
        retry_table[series["SeriesInstanceUID"]]["AccessionNumber"] = series["AccessionNumber"]
        retry_table[series["SeriesInstanceUID"]]["PatientID"] = series["PatientID"]
        retry_table[series["SeriesInstanceUID"]]["StudyDate"] = series["StudyDate"]
        retry_table[series["SeriesInstanceUID"]]["Modality"] = series["Modality"]
        retry_table[series["SeriesInstanceUID"]]["NumberOfSeriesRelatedInstances"] = series["NumberOfSeriesRelatedInstances"]

    return retry_table

def get_max_poll(file_count: int, default_poll: int) -> int:
    """
    Adjust polling to CUBE based on no. of series related instances
    """
    if file_count == 0:
        total_polls = 0
    elif file_count < 100:
        total_polls = default_poll
    else:
        # 5 polls per 100 files
        total_polls = default_poll * (file_count // 100)

    return total_polls

# Recursive method to check on registration and then run anonymization pipeline
async def check_registration(options: Namespace, retry_table: dict, client: PACSClient, contains_errors: bool=False):
    # null check
    if len(retry_table) == 0:
        return contains_errors

    clone_retry_table = copy.deepcopy(retry_table)

    for series_instance in retry_table.keys():
        LOG(f"Polling CUBE for series: {series_instance}.")
        registered_series_count = client.get_pacs_registered({'SeriesInstanceUID':series_instance})

        file_count: int = retry_table[series_instance]["NumberOfSeriesRelatedInstances"]

        # poll CUBE at regular interval for the status of file registration
        poll_count: int = 0
        total_polls: int = get_max_poll(file_count, options.maxPoll)
        wait_poll: int = options.pollInterval
        while registered_series_count < 1 and poll_count < total_polls:
            poll_count += 1
            time.sleep(wait_poll)
            registered_series_count = client.get_pacs_registered({'SeriesInstanceUID':series_instance})
            LOG(f"{registered_series_count} series found in CUBE.")

        # check if polling timed out before registration is finished
        if registered_series_count == 0 and clone_retry_table[series_instance]["retry"] > 0:
            LOG(f"PACS series registration unsuccessful. Retrying retrieve for {series_instance}.")
            # retry retrieve
            retrieve_response = pfdcm.retrieve_pacsfiles(retry_table[series_instance],
                                                         options.PACSurl, options.PACSname)

            # save retry file
            srs_json_file_path = os.path.join(options.outputdir,
                                                  f"{series_instance}_retrieve_retry_{clone_retry_table[series_instance]["retry"]}.json")
            clone_retry_table[series_instance]["retry"] -= 1
            with open(srs_json_file_path, 'w', encoding='utf-8') as jsonf:
                jsonf.write(json.dumps(retrieve_response, indent=4))
            continue

        if registered_series_count:
            LOG(f"Series {series_instance} successfully registered to CUBE.")
            send_params = {
                "neuro_dcm_location": options.neuroDicomLocation,
                "neuro_anon_location": options.neuroAnonLocation,
                "neuro_nifti_location": options.neuroNiftiLocation,
                "folder_name": options.folderName,
                "recipients": options.recipients,
                "smtp_server": options.SMTPServer
            }
            dicom_dir = client.get_pacs_files({'SeriesInstanceUID': series_instance})
            series_data = json.dumps(retry_table[series_instance])

            # create ChRIS Client Object
            cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
            d_ret = await cube_con.anonymize(dicom_dir, send_params, options.pluginInstanceID, series_data)
            if d_ret.get('error'):
                contains_errors = True
        clone_retry_table.pop(series_instance)

    await check_registration(options, clone_retry_table, client, contains_errors)
    return contains_errors

if __name__ == '__main__':
    main()
