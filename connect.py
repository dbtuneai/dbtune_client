import json
import time
import datetime
import logging
import requests
from requests.exceptions import HTTPError
import os
import getpass

class Connect:
    def __init__(self, endpoint, api_key, db_id):
        logging.info("Connecting to db {} {}".format(api_key, db_id))
        endpoint = endpoint + "db/"
        self.api_key = api_key
        self.db_id = db_id
        self.database_instance_endpoint = endpoint + self.db_id + "/database-instance"
        self.client_info_endpoint = endpoint + self.db_id + "/client-info"
        self.tuning_session_id_endpoint = endpoint + self.db_id + "/tuning-session-id"
        self.iteration = None

    def get_database_instance(self):
        try:
            response = requests.get(self.database_instance_endpoint,
                                    headers={'X-HYPER-API-KEY': self.api_key}, )
            response.raise_for_status()
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6
        else:
            db_instance_info = response.json()
            return db_instance_info

    def post_client_info(self, client_info):
        try:
            requests.post(self.client_info_endpoint, json=client_info, headers={'X-HYPER-API-KEY': self.api_key}, )
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6

    def get_tuning_session_id(self):
        try:
            response = requests.get(self.tuning_session_id_endpoint,
                                    headers={'X-HYPER-API-KEY': self.api_key}, )
            response.raise_for_status()
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6
        else:
            tuning_session_id = response.json()
            return tuning_session_id