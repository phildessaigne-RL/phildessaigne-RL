import json
import os
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
import sys
import pandas as pd
from rl_utils import fetch_all_data_from_api_json, make_api_call_json, APIException
# from rl_report_util import launch_holdings_report, fetch_export_dataset_response
import logging
import openpyxl
import yaml
import joblib
import pandas_datareader.data as web
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import time
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

PROGRAM_NAME = "excercise1"
if not os.path.exists('output/' + PROGRAM_NAME):
    os.mkdir('output/' + PROGRAM_NAME)

global API_KEY
global API_URL
global session

LOG_FORMAT = '[%(asctime)s][%(name)-12s][%(levelname)-8s] - %(message)s'
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(handler)
logger.setLevel('DEBUG')
START_DATETIME = datetime.now()


# get log in and run in 10c
# find a larger group, go into config file

def begin_run(pgm_name):
    global START_DATETIME
    START_DATETIME = datetime.now()
    start_time = START_DATETIME.strftime("%H:%M:%S")
    print(pgm_name, 'start run:', start_time)


def end_run(pgm_name):
    current_date = datetime.now()
    current_time = current_date.strftime("%H:%M:%S")
    elapsed_time = current_date - START_DATETIME
    print(pgm_name, 'end run:', current_time, ' elapsed time:' + str(elapsed_time))


def load_config(name):
    with open("config/sz_config.yaml", 'r') as stream:
        try:
            config_file = yaml.safe_load(stream)
            global API_KEY
            global API_URL
            API_URL = config_file['api_url']
            API_KEY = config_file['api_key']
            return config_file[name]
        except yaml.YAMLError as exc:
            print(exc)


def main():
    begin_run(PROGRAM_NAME)
    config_settings = load_config(PROGRAM_NAME)
    price_source_id = config_settings['price_source_id']

    if not config_settings['run_date_end']:
        run_date_start = datetime.strptime(config_settings['run_date_start'], '%Y-%m-%d')
        run_date_end = datetime.date.now()
    else:
        run_date_start = datetime.strptime(config_settings['run_date_start'], '%Y-%m-%d')

        run_date_end = datetime.strptime(config_settings['run_date_end'], '%Y-%m-%d')

    end_run(PROGRAM_NAME)


if __name__ == '__main__':
    sys.exit(main())
