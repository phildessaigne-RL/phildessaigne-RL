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

PROGRAM_NAME = "excercise2"
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

def get_port_group_id_from_name(port_group_name):
    port_groups_xref = get_port_groups_xref()
    port_group_id = port_groups_xref.loc[port_groups_xref['group_name'] == port_group_name, 'group_id'].iloc[0]
    return port_group_id


def get_port_groups_xref():
    rl_resp = make_api_call_json("FetchFullPortfolioTrees", {}, API_URL, API_KEY)
    edges = rl_resp['data']['portfolioTrees']['edges']
    trees = [(e['node']['tree']) for e in edges]

    df_groups = pd.DataFrame()
    for x, tree in enumerate(trees):
        tree = json.loads(tree)
        df_groups = parse_trees(tree, df_groups)
    df_groups = df_groups.transpose()
    return df_groups


def parse_trees(node, df_groups):
    if isinstance(node, dict):
        if 'root' in node:
            node = node['root']

        if 'children' in node:
            node = node['children']
            df_groups = parse_trees(node, df_groups)
        elif ('id' and 'name' in node) and ('children' not in node):
            data = {'group_name': node['name'], 'group_id': node['id']}
            df_groups = pd.concat([df_groups, pd.DataFrame.from_dict(data, orient='index')], axis=1,
                                  ignore_index=True)
            return df_groups
        else:
            for item in node.items():
                if isinstance(item[1], dict):
                    df_groups = parse_trees(item[1], df_groups)

    return df_groups

def launch_holdings_report(as_of_date, portfolio_id):
    launch_holdings_report_variables = {
        'portfolioEntities': [portfolio_id],
        'asOfDate': as_of_date,
        'columnsSelector': [
            {"columnName": "PortfolioId"},
            {'columnName': "Quantity"},
            {'columnName': 'SecurityType'},
            {'columnName': "SecurityDescription"},
            {'columnName': "SecurityMarketPrice"},
            {'columnName': "MarketValue"},
            {"columnName": "SecurityCusip"},
        ],
    }
    launch_holdings_report_response = make_api_call_json("LaunchHoldingsReport",
                                                         launch_holdings_report_variables, API_URL,
                                                         API_KEY)
    jobId = launch_holdings_report_response["data"]["launchAccountingReportResult"]["jobId"]
    logger.debug(f'LaunchHoldingsReport - JobId: {jobId}')
    return jobId


def get_export_data_set(job_id):

    result_json = fetch_export_dataset_response(job_id)
    result_json_df = pd.read_json(result_json)
    return result_json_df


def fetch_export_dataset_response(job_id):
    result_json_text = {}
    fetch_export_dataset_response_variables = {
        'jobId': job_id
    }

    # pool the api till success or failure

    try:
        poll_flag = True
        while poll_flag:
            job_response = make_api_call_json("FetchExportDataSetResponse", fetch_export_dataset_response_variables,
                                              API_URL, API_KEY)
            if job_response['data']['result']['jobStatus'] == "SUCCEEDED":
                jobResultsURL = job_response['data']['result']['presignedUrl']
                file = requests.get(jobResultsURL)
                result_json_text = file.text
                poll_flag = False
            elif job_response['data']['result']['jobStatus'] == "FAILED":
                logger.error(f'Job Failed : ,{job_id}, with reason: , {job_response["data"]["result"]["failureReason"]}')
                poll_flag = False
                exit()
            else:
                time.sleep(2)

    except Exception as error:
        print(error)
        sys.exit(0)
    return result_json_text

def sort_lines_by_security_type(json_result_df):

    column_names = ["MarketValue", "PortfolioId","Quantity", "SecurityCusip", "SecurityDescription", "SecurityMarketPrice", "SecurityType"]
    securities = pd.DataFrame(columns=column_names)
    cash_equivalencies = pd.DataFrame(columns=column_names)
    for index, line_item in json_result_df.iterrows():
        if line_item['SecurityType'] == "COMMON-STOCK":
            securities = securities.append(line_item, ignore_index=True)
        elif line_item['SecurityType'] == "CURRENCY":
            cash_equivalencies= cash_equivalencies.append(line_item, ignore_index=True)
    grouped_summed_securities = securities.groupby(["SecurityDescription"])[["MarketValue"]].sum().reset_index()
    consolidated_holdings = cash_equivalencies.drop(columns=["PortfolioId","Quantity", "SecurityCusip", "SecurityMarketPrice", "SecurityType"]).append(grouped_summed_securities)

    return consolidated_holdings

def main():
    begin_run(PROGRAM_NAME)
    config_settings = load_config(PROGRAM_NAME)
    run_date = config_settings['run_date']
    portfolio_group_name = config_settings['port_group_name']
    output_file_name = config_settings['output_file_name']

    group_id = get_port_group_id_from_name(portfolio_group_name)
    job_id = launch_holdings_report(run_date, group_id)
    json_result_df = get_export_data_set(job_id)
    consolidated_holdings = sort_lines_by_security_type(json_result_df)
    consolidated_holdings.to_excel(output_file_name, sheet_name='Holdings')
    end_run(PROGRAM_NAME)


if __name__ == '__main__':
    sys.exit(main())
