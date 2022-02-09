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
import multiprocessing
from joblib import Parallel, delayed
import pandas_datareader.data as web
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import time
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

PROGRAM_NAME = "OptimizedSecurityPriceCheck"
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

def create_price_dataframes_output(price_list, securities_df):
    securities_with_prices = {}
    securities_without_prices = {}
    for security in price_list:
        securities_data = securities_df.loc[security['id'],:]
        security_dic = {}
        if 'price' in security.keys():
            security_dic['price'] = security['price']
            security_dic['priceid'] = security['priceid']
            security_dic['ticker'] = securities_data['ticker']
            security_dic['cusip']  = securities_data['cusip']
            securities_with_prices[security_dic['ticker']] = security_dic
        else:
            security_dic['ticker'] = securities_data['ticker']
            security_dic['cusip'] = securities_data['cusip']
            securities_without_prices[security_dic['ticker']] = security_dic

        securities_with_prices_pd = pd.DataFrame.from_dict(securities_with_prices, orient='index')
        securities_without_prices_pd = pd.DataFrame.from_dict(securities_without_prices, orient='index')

    return securities_with_prices_pd, securities_without_prices_pd



def get_security_price_by_date(securityid, run_date) -> dict:
    security_price = {}
    variables = {'securityId': securityid, 'priceDate': run_date}
    try:
        price_response = make_api_call_json("FetchPriceOnSecurityByDate", variables, API_URL, API_KEY)
        security_price['date'] = price_response['data']['price']['priceDate']
        if security_price['date'] == run_date:
            security_price['id'] = securityid
            security_price['priceid'] = price_response['data']['price']['id']
            security_price['price'] = price_response['data']['price']['price']
            security_price['date'] = run_date
        else:
            security_price['id'] = securityid
            security_price['date'] = run_date
    except APIException as error:
        if error.errors[0]['subcode']['title'] == 'Price':
            security_price['date'] = run_date
            security_price['id'] = securityid

    return security_price


def get_securities_data(securities_response) -> dict:
    security_info = {}
    securities_id_list = []
    for index, sleeve in enumerate(securities_response):
        security_info[sleeve['node']['id']] = {
            'id': sleeve['node']['id'],
            'cusip': sleeve['node']['cusip'],
            'ticker': sleeve['node']['ticker']
        }
        securities_id_list.append(sleeve['node']['id'])
    return security_info, securities_id_list


def get_securities_df():
    # call api
    all_securities_sleeves_response = fetch_all_data_from_api_json("FetchSecurities", {}, API_URL, API_KEY)
    securities_data, securities_id_list = get_securities_data(all_securities_sleeves_response)
    securities_df = pd.DataFrame.from_dict(securities_data, orient='index')

    return securities_df, securities_id_list


def main():
    begin_run(PROGRAM_NAME)
    config_settings = load_config(PROGRAM_NAME)
    run_date_start = config_settings['run_date_start']



    securities_df, securities_all_list = get_securities_df()
    print("There are " + str(len(securities_all_list)) + " securities in RL Tenant")
    # for security in securities_all_list:
    #     security_price = get_security_price_by_date(security, run_date_start)

    num_cores = multiprocessing.cpu_count()
    print('PARALLEL API CREATES WITH ' + str(num_cores) + ' CORES')
    price_list = Parallel(n_jobs=num_cores)(
        delayed(get_security_price_by_date)(i, run_date_start) for i in securities_all_list)

    security_with_prices, security_without_prices = create_price_dataframes_output(price_list, securities_df)

    security_with_prices.to_excel("output/securities_with_prices.xlsx", sheet_name='Securities')
    security_without_prices.to_excel("output/securities_without_prices.xlsx", sheet_name='Securities')

    end_run(PROGRAM_NAME)

if __name__ == '__main__':
    sys.exit(main())
