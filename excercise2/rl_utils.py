from typing import List
import requests
from datetime import datetime
import logging
import json

logger = logging.getLogger()


class APIException(Exception):
	"""This error is thrown when we the API returns errors.
	"""

	def __init__(self, api_name: str, errors: List[dict]):
		self.message = f"API {api_name} returned errors"
		self.api_name = api_name
		self.errors = errors
		super().__init__(self.message)

		print(errors)


def get_piped_ids(arr) -> str:
	return '|'.join([x['id'] for x in arr])


def get_piped_values(arr, fld) -> str:
	return '|'.join([str(x[fld]).strip() for x in arr])


def check_retry_errors(errors) -> bool:
	first_message = errors[0]['message']
	retry_errors = [
		'Body Corrupted'
	]
	for retry_text in retry_errors:
		if retry_text in first_message:
			return True

	return False


def make_api_call_json(operation: str, variables: str, endpoint: str, api_key: str) -> dict:
	url = endpoint
	payload = {}
	payload['operation'] = operation
	payload['variables'] = variables
	payload_json = json.dumps(payload)
	headers = {
		'x-api-key': api_key,
		'content-type': "application/json"
	}
	try:
		logger.debug(f'Request Payload - {payload_json}')
		api_response = requests.request("POST", url, data=payload_json, headers=headers)
		response_json = api_response.json()
		# print(response.elapsed.total_seconds())
		errors = response_json.get("errors", None)
		data = response_json.get("data", None)
		if errors is not None or data is None:

			logger.debug(f'*RETRYING AFTER FAILURE*')
			api_response = requests.request("POST", url, data=payload_json, headers=headers)
			response_json = api_response.json()
			# print(response.elapsed.total_seconds())
			errors = response_json.get("errors", None)
			data = response_json.get("data", None)
			if errors is not None or data is None:
				logger.error(f'Response Payload after retry - {response_json}')
				raise APIException(operation, errors)

		logger.debug(f'Response Payload - {response_json}')

		return response_json
	except requests.exceptions.RequestException as error:
		return {"errors": "Error calling: " + operation}


def fetch_all_data_from_api_json(api_name: str, vars, endpoint: str, api_key: str) -> List[dict]:
	max_per_page = 100
	results = []
	vars["first"] = max_per_page

	has_next_page = True
	api_page_number = 1
	while has_next_page:
		has_next_page = False
		api_response = make_api_call_json(api_name, vars, endpoint, api_key)
		errors = api_response.get("errors", None)
		data = api_response.get("data", None)
		if errors is not None or data is None:
			raise APIException(api_name, errors)

		payload = data[list(data)[0]]
		results += payload["edges"]
		has_next_page = payload["pageInfo"].get("hasNextPage", False)
		if has_next_page:
			after = payload["pageInfo"]["endCursor"]
			vars["first"] = max_per_page
			vars["after"] = after
			api_page_number += 1

	return results