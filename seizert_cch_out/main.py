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

PROGRAM_NAME = "SeizertCchOut"
if not os.path.exists('output/' + PROGRAM_NAME):
    os.mkdir('output/' + PROGRAM_NAME)

LOG_FORMAT = '[%(asctime)s][%(name)-12s][%(levelname)-8s] - %(message)s'
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(handler)
logger.setLevel('DEBUG')
START_DATETIME = datetime.now()


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
    with open("config/sz_cch_config.yaml", 'r') as stream:
        try:
            global output_file_name
            global override_run_date
            global watch_list_definition
            global API_KEY
            global API_URL

            config_file = yaml.safe_load(stream)
            API_URL = config_file['api_url']
            API_KEY = config_file['api_key']
            output_file_name = config_file['SeizertCchOut']['output_file_name']
            override_run_date = config_file['SeizertCchOut']['override_run_date']
            watch_list_definition = config_file['SeizertCchOut']['watch_list_definition']

            return config_file[name]
        except yaml.YAMLError as exc:
            print(exc)

def find_cusip_for_instruments(watchlist_instruments_response, securities_df):
    instruments_cusip = []
    for instrument in watchlist_instruments_response:
        instrumentId = instrument['node']['instrumentId']
        instruments_cusip.append(securities_df.loc[instrumentId]['ticker'])
    instruments_cusip_df = pd.DataFrame(instruments_cusip, columns = ['CUSIP'] )
    return instruments_cusip_df

def fetch_watchlist_instruments(securities_df, watch_list_definition_id, override_run_date):
    variables = {
        "watchlistDefinitionId": watch_list_definition_id,
        "date": override_run_date
    }
    watchlist_instruments_response = fetch_all_data_from_api_json("FetchWatchlistInstruments", variables, API_URL, API_KEY)
    instruments_cusip_df = find_cusip_for_instruments(watchlist_instruments_response, securities_df)
    return instruments_cusip_df

def get_watchlist_definition_id(watch_list_definition, all_watch_list_definition_response):
    for edge in all_watch_list_definition_response:
        if watch_list_definition == edge['node']['name']:
            watch_list_definition_id = edge['node']['id']
            break
    return watch_list_definition_id


def get_watchlist_definitions(watch_list_definition):
    all_watch_list_definition_response = fetch_all_data_from_api_json("FetchWatchlistDefinitions", {}, API_URL, API_KEY)
    watch_list_definition_id = get_watchlist_definition_id(watch_list_definition, all_watch_list_definition_response)

    return watch_list_definition_id


def get_securities_data(securities_response) -> dict:
    security_info = {}
    for index, sleeve in enumerate(securities_response):
        security_info[sleeve['node']['instrumentId']] = {
            'ticker': sleeve['node']['ticker'],
            'cusip': sleeve['node']['cusip'],
        }
    return security_info


def get_securities_df():
    # call api
    all_securities_sleeves_response = fetch_all_data_from_api_json("FetchSecurities", {}, API_URL, API_KEY)
    securities_data = get_securities_data(all_securities_sleeves_response)
    securities_df = pd.DataFrame.from_dict(securities_data, orient='index')
    return securities_df


def main():
    begin_run(PROGRAM_NAME)
    load_config(PROGRAM_NAME)

    if not override_run_date:
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')
        output_file_date = yesterday.strftime('%m%d')
        run_date = yesterday_str
    else:
        run_date = override_run_date
        output_file_date = override_run_date[5:10]
        output_file_date = output_file_date.replace('-', '')

    print(f'Running {PROGRAM_NAME} for {run_date}...')

    securities_df = get_securities_df()
    watch_list_definition_id = get_watchlist_definitions(watch_list_definition)
    instruments_cusip_df = fetch_watchlist_instruments(securities_df, watch_list_definition_id, override_run_date)
    print(instruments_cusip_df)

    if len(instruments_cusip_df) == 0:
        raise Exception(f'Holdings report for {run_date} did not return any open positions')
    output_df = instruments_cusip_df.drop_duplicates(subset='CUSIP', keep="first")

    output_path_file = output_file_name.replace('MMDD', output_file_date)
    output_df.to_csv(output_path_file, sep='\t', encoding='iso-8859-9', index=False, header=True)
    end_run(PROGRAM_NAME)


if __name__ == '__main__':
    sys.exit(main())
