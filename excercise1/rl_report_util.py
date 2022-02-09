import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
import requests
import config
from rl_utils import make_api_call, fetch_all_data_from_api
logger = logging.getLogger()
global API_URL
global API_KEY


def launch_and_retrieve_sma_export_job(api_url, api_key):
    global API_KEY
    global API_URL
    API_KEY = api_key
    API_URL = api_url
    job_id = launch_sma_export_job()
    job_results = fetch_sma_export_job_results(job_id)
    return job_results


def launch_sma_export_job():
    launch_sma_export_job_response = make_api_call_json("LaunchSMAExportJob",
                                                        {}, API_URL,
                                                        API_KEY)
    jobId = launch_sma_export_job_response["data"]["smaExportJob"]["id"]
    logger.debug(f'LaunchSMAExportJob - JobId: {jobId}')
    return jobId


def fetch_successful_sma_export_job_results(job_id):
    fetch_sma_export_job_results_variables = {
        'smaExportResultId': job_id
    }
    fetch_sma_export_job_results_edges = fetch_all_data_from_api_json("FetchSMAExportJobResults",
                                                                      fetch_sma_export_job_results_variables, API_URL,
                                                                      API_KEY)
    return fetch_sma_export_job_results_edges


def fetch_sma_export_job_results(job_id):
    result_json = {}
    fetch_sma_export_job_status_variables = {
        'smaExportJobId': job_id
    }

    # pool the api till success or failure

    try:
        poll_flag = True
        while poll_flag:
            job_status = make_api_call_json("FetchSMAExportJobStatus", fetch_sma_export_job_status_variables,
                                            API_URL, API_KEY)
            if job_status['data']['smaExportJobStatusResult']['status'] == "PROCESSED":
                result_id = job_status['data']['smaExportJobStatusResult']['id']
                result_json = fetch_successful_sma_export_job_results(result_id)

                poll_flag = False
            elif job_status['data']['smaExportJobStatusResult']['status']  == "FAILED":
                logger.error(f'Job Failed : ,{job_id}, with reason: , {job_status["data"]["smaExportJobStatusResult"]["failureReason"]}')
                poll_flag = False
                exit()
            else:
                time.sleep(2)

    except Exception as error:
        print(error)
        sys.exit(0)

    return result_json


def launch_holdings_report(as_of_date):
    launch_holdings_report_variables = {
        'portfolioEntities': ['cmv1:portfolio:PORT_1019SMA'],
        'asOfDate': as_of_date,
        'columnsSelector': [
            {'columnName': 'SecurityType'},
            {'columnName': "MarketValue"}
        ]
    }
    launch_holdings_report_response = make_api_call_json("LaunchHoldingsReport",
                                                         launch_holdings_report_variables, API_URL,
                                                         API_KEY)
    jobId = launch_holdings_report_response["data"]["launchAccountingReportResult"]["jobId"]
    logger.debug(f'LaunchHoldingsReport - JobId: {jobId}')
    return jobId


def launch_holdings_report(as_of_date, portfolio_id):
    launch_holdings_report_variables = {
        'portfolioEntities': [portfolio_id],
        'asOfDate': as_of_date,
        'columnsSelector': [
            {'columnName': 'SecurityType'},
            {'columnName': "MarketValue"}
        ]
    }
    launch_holdings_report_response = make_api_call_json("LaunchHoldingsReport",
                                                         launch_holdings_report_variables, API_URL,
                                                         API_KEY)
    jobId = launch_holdings_report_response["data"]["launchAccountingReportResult"]["jobId"]
    logger.debug(f'LaunchHoldingsReport - JobId: {jobId}')
    return jobId


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
