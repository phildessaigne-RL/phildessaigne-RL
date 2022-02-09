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


def edit_price_on_security(securityid, price, pricedate, pricesourceid, priceid):
    variables = {
        "securityId": securityid,
        "priceId": priceid,
        "editPriceOnSecurityInput": {
            "price": price,
            "priceDate": pricedate,
            "priceSourceId": pricesourceid,
            "priceType": "EOD"
        }}
    price_response = make_api_call_json("EditPriceOnSecurity", variables, API_URL, API_KEY)


def create_price_on_security(securityid, price, pricedate, price_source_id):
    variables = {
        "id": securityid,
        "createPriceOnSecurityInput": {
            "price": price,
            "priceDate": pricedate,
            "priceSourceId": price_source_id,
            "priceType": "EOD"
        }}
    price_response = make_api_call_json("CreatePriceOnSecurity", variables, API_URL, API_KEY)


def get_security_price_from_yahoo(ticker, run_date_start, run_date_end):
    df = web.DataReader(ticker, data_source='yahoo', start=run_date_start, end=run_date_end)
    return df['Close']


def get_security_price_by_date(securityid, run_date) -> dict:
    security_price = {}
    variables = {'securityId': securityid, 'priceDate': run_date}
    try:
        price_response = make_api_call_json("FetchPriceOnSecurityByDate", variables, API_URL, API_KEY)
        security_price['priceid'] = price_response['data']['price']['id']
        security_price['price'] = price_response['data']['price']['price']
        security_price['date'] = price_response['data']['price']['priceDate']
        if security_price['date'] != run_date:
            security_price = {}
    except:
        pass
    return security_price


def get_securities_data(securities_response) -> dict:
    security_info = {}
    for index, sleeve in enumerate(securities_response):
        security_info[sleeve['node']['ticker']] = {
            'id': sleeve['node']['id'],
            'security.instrumentId': sleeve['node']['instrumentId'],
            'ticker': sleeve['node']['ticker']
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
    config_settings = load_config(PROGRAM_NAME)
    price_source_id = config_settings['price_source_id']

    if not config_settings['run_date_end']:
        run_date_start = datetime.strptime(config_settings['run_date_start'], '%Y-%m-%d')
        run_date_end = datetime.date.now()
    else:
        run_date_start = datetime.strptime(config_settings['run_date_start'], '%Y-%m-%d')
        run_date_end = datetime.strptime(config_settings['run_date_end'], '%Y-%m-%d')

    securities_df = get_securities_df()

    for index, security in securities_df.iterrows():
        try:
            if security['ticker'] != None:
                yahoo_security_price_series = get_security_price_from_yahoo(security['ticker'], run_date_start, run_date_end)
                print(yahoo_security_price_series)
                for date, price in yahoo_security_price_series.iteritems():
                    try:
                        date = date.strftime('%Y-%m-%d')
                        yahoo_security_price = float("{:.2f}".format(price))
                        security_price = get_security_price_by_date(security['id'], date)
                        if security_price == {}:
                            try:
                                create_price_on_security(security['id'], yahoo_security_price, date, price_source_id)
                                print("Security " + security['ticker'] + " updated")
                            except:
                                print("Price for security " + security['ticker'] + " failed to create for " + str(date))
                        elif security_price != {} and security_price['price'] != yahoo_security_price:
                            try:
                                edit_price_on_security(security['id'], yahoo_security_price, date, price_source_id,
                                                       security_price['priceid'])
                                print('EOD Price in Yahoo used to overwrite EOD price in Ridgeline for security ' + security[
                                    'ticker'])
                            except:
                                print("Price for security " + security['ticker'] + " failed to edit for " + str(date))
                        else:
                            print("Price for security " + security['ticker'] + " already exists and is up to date in Ridgeline for " + str(date))
                    except:
                        print("Price for security " + security['ticker'] + " was not updated due to an error for " + str(date) + " , please review logs")
        except:
            print("Price for security " + security['ticker'] + " was not updated due to an error in getting EOD price for Yahoo or updating in ridgeline - please review logs for more info")


if __name__ == '__main__':
    sys.exit(main())
